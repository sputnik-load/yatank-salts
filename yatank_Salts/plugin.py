# -*- coding: utf-8 -*-

import sys
import os
import signal
import importlib
import getpass
import datetime
import glob
from urllib import quote
from yandextank.core import AbstractPlugin
from yandextank.plugins.Aggregator import AggregateResultListener
from tank_api_client import SaltsClient, TankClient, TankClientError


def check_for_active_tank(tank_host, port=8888):
    try:
        TankClient(tank_host, port)
    except TankClientError, exc:
        raise RuntimeError("The yandex-tank-api-server is working "
                           "incorrectly on the %s:%s "
                           "host now: %s" % (tank_host, port, exc))


class SaltsPlugin(AbstractPlugin, AggregateResultListener):

    SECTION = 'salts'
    DEFAULT_RESULT_QUERY = "scpath={scenario_path}&" \
                           "st=pass,fail,unknown"
    # Don't save test results with test duration 
    # < 3 minutes, if test was broken by user with ^C
    DB_SAVE_LOW_DURATION_BOUND = 180  
    MAX_FILE_SIZE = 75000000
    DATEFORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def get_key():
        return __file__

    def __init__(self, core):
        AbstractPlugin.__init__(self, core)
        self.scenario_id = None
        self.data_plugin = None
        self.current_data = None
        self.shooting_status = None
        self.salts_api_client = None
        self.api_url = None
        self.api_key = None
        self.is_result_save = True
        self.new_id = -1
        self.yatank_signal_handler = None
        self.lock_dir = None
        self.used_for_web = None

    def get_available_options(self):
        return ['scenario_id', 'scenario_path',
                'api_url', 'api_user', 'api_key', 'api_host'
               ]

    def configure(self):
        try:
            data_plugin_module = self.get_option('data_plugin_module')
            data_plugin_class = self.get_option('data_plugin_class')
            m = importlib.import_module(data_plugin_module)
            c = getattr(m, data_plugin_class)
            self.data_plugin = self.core.get_plugin_of_type(c)
        except Exception, exc:
            raise RuntimeError("No plugin providing data: %s" % exc)
        self.current_data = self.data_plugin.get_data('configure')
        self.api_url = self.get_option('api_url', '')
        self.api_user = self.get_option('api_user', '')
        self.api_key = self.get_option('api_key', '')
        self.api_host = self.get_option('api_host', '')
        self.result_query = self.get_option('results_url_query',
                                            SaltsPlugin.DEFAULT_RESULT_QUERY)
        self.provide_scenario_id()
        self.provide_used_for_web()

        if self.current_data['yandex_api_server']:
            self.lock_dir = self.core.tank_worker.core.get_lock_dir()
        else:
            self.yatank_signal_handler = signal.signal(signal.SIGINT,
                                                       self.signal_handler)
            self.lock_dir = self.core.get_lock_dir()
        if not self.current_data['yandex_api_server'] and not self.api_key:
            self.data_plugin.set_value('user_name', getpass.getuser())

    def prepare_test(self):
        self.current_data = self.data_plugin.get_data('prepare_test')
        if self.current_data['yandex_api_server']:
            console_api_key = self.current_data["console_default_api_key"]
            if self.api_key == console_api_key:
                raise RuntimeError("It's forbidden to run the 'runload' "
                                   "script with default api key (from %s).",
                                   console_api_key)
            check_for_active_tank(self.current_data["load_gen"])
        else:
            if not self.api_key:
                self.api_key = self.current_data['console_default_api_key']
        self.salts_api_client = SaltsClient(self.api_url,
                                            self.api_key)

        if not self.salts_api_client:
            raise RuntimeError("Client for salts is not provided.")
        if self.current_data['yandex_api_server'] and (not self.used_for_web):
            raise RuntimeError("The config cannot used via web.")
        if not self.scenario_id:
            try:
                self.provide_scenario_id(from_salts=True)
                self.log.warning("Provided ini-config doesn't contain "
                                "'scenario_id' option. It must contain "
                                "salts.scenario_id=%s to start corresponding "
                                "test via web." % self.scenario_id)
            except Exception, exc:
                raise RuntimeError("The load.ini scenario does not contain the "
                                   "'scenario_id' value. Also it's impossible "
                                   "to get it from salts because the 'scenario_path' "
                                   "option isn't provided, it is incorrect "
                                   "or the current user '%s' "
                                   "hasn't got sufficient permissions. "
                                   "Some details: %s" % (self.api_user, exc))

        sh_data = {'status': 'P',
                   'scenario_id': self.scenario_id,
                   'tank_host': self.current_data['load_gen'],
                   'alt_name': self.current_data['user_name'],
                   'force_run': self.current_data['force_run'],
                   'ticket_id': self.current_data['ticket_id']
                  }
        self.salts_api_client.update_shooting(self.current_data['session_id'], **sh_data)
        self.data_plugin.set_value(
            'user_name', self.salts_api_client.get_user_name())
        self.data_plugin.set_value(
            "scenario_path", self.salts_api_client.get_git_scenario_path())
        self.data_plugin.set_value(
            'results_url', self.generate_results_url(
                                'results', self.current_data['scenario_path']))
        self.data_plugin.set_value(
            'results_graph_url', self.generate_results_url(
                                    'results/graph', self.current_data['scenario_path']))

    def start_test(self):
        self.current_data = self.data_plugin.get_data('start_test')
        self.shooting_status = 'R'
        sh_data = {'status': self.shooting_status,
                   'start': self.current_data['start_time']['timestamp'],
                   'planned_duration': self.current_data['planned_duration']['seconds'],
                   'force_run': self.current_data['force_run']}
        web_console = self.current_data.get('web_console')
        if web_console:
            sh_data['web_console_port'] = web_console['port']
        self.salts_api_client.update_shooting(self.current_data['session_id'], **sh_data)

    def end_test(self, retcode):
        self.current_data = self.data_plugin.get_data('end_test')
        if self.shooting_status == 'R':
            self.shooting_status = 'F'
        sh_data = {'status': self.shooting_status,
                   'finish': self.current_data['end_time']['timestamp'],
                   'force_run': self.current_data['force_run']}
        self.salts_api_client.update_shooting(self.current_data['session_id'], **sh_data)
        return retcode

    def run(self):
        pass

    def post_process(self, retcode):
        self.save_test_result()
        return retcode

    def provide_scenario_id(self, from_salts=False):
        if not from_salts:
            self.scenario_id = self.get_option('scenario_id', '')
            if not self.scenario_id:
                self.scenario_id = self.get_option('test_ini_id', '')
                if self.scenario_id:
                    self.log.warning("'test_ini_id' option is deprecated. "
                                    "It won't be supported in future versions. "
                                    "Please use 'scenario_id' option "
                                    "instead of it.")
            return
        scenario_path = self.get_option('scenario_path', '')
        if not scenario_path:
            return
        self.scenario_id = self.salts_api_client.provide_scenario_id(scenario_path)

    def provide_used_for_web(self):
        bool_values = {"true": True,
                       "false": False}
        str_value = self.get_option("used_for_web", "true").lower()
        self.used_for_web = bool_values.get(str_value, True)

    def get_scenario_id(self):
        if not self.scenario_id:
            self.log.warning("The 'scenario_id' value is empty. It is defined "
                             "after configure stage if scenario_id option "
                             "is not empty.")
        return self.scenario_id

    def generate_results_url(self, path, scenario_path):
        results_url = ''
        try:
            result_query_str = self.result_query.format(
                scenario_path=quote(scenario_path))
            results_url = "{api_host}/?{result_query}".format(
                api_host="%s/%s" % (self.api_host, path),
                result_query=result_query_str)
        except Exception, exc:
            self.log.warning("Results URL hasn't been generated. "
                                "Check the 'results_url_query' option in "
                                "the 'salts_report' section.")
        return results_url

    def generate_edit_results_url(self):
        if self.save_test_result() == 0:
            return ''
        return "{api_host}/admin/salts/testresult/?id={id}".format(
                    api_host=self.api_host, id=self.new_id)

    def save_test_result(self):
        def dp_value(key):
            return self.data_plugin.calc_value(key, 'post_process')
        if self.new_id >= 0:
            return self.new_id
        if not self.is_result_save:
            self.log.info("Don't save results to salts - "
                          "test was interrupted by Ctrl+C")
            self.new_id = 0
            return 0

        start_time_dt = dp_value('start_time')['datetime']
        end_time_dt = dp_value('end_time')['datetime']
        result_data = {
            'session_id': dp_value('session_id'),
            'dt_start': start_time_dt.strftime(SaltsPlugin.DATEFORMAT),
            'dt_finish': end_time_dt.strftime(SaltsPlugin.DATEFORMAT),
            'group': dp_value('test_group'),
            'test_name': dp_value('test_name'),
            'target': dp_value('target'),
            'version': dp_value('system_version'),
            'rps': dp_value('rps'),
            'q99': dp_value('q99'),
            'q90': dp_value('q90'),
            'q50': dp_value('q50'),
            'http_errors_perc': dp_value('http_perc_str'),
            'net_errors_perc': dp_value('net_perc_str'),
            'graph_url': dp_value('end_graph_url'),
            'generator': dp_value('load_gen'),
            'user': dp_value('user_name'),
            'ticket_id': dp_value('ticket_id'),
            'mnt_url': dp_value('ltm_url'),
            'comments': '',
            'test_status': 'unk',
            'scenario_path': dp_value('scenario_path'),
            'generator_types': dp_value('gen_type')['list']
        }

        if self.salts_api_client:
            try:
                self.new_id = self.salts_store_test_result(**result_data)
            except Exception, exc:
                self.log.error("Error sending results to salts: " + str(exc))
                if hasattr(exc, 'content'):
                    self.log.error("Exception content: " + str(exc.content))
                self.log.debug("Exception : " + repr(exc))
                self.new_id == 0
        else:
            self.log.warning("Results haven't been saved to salts: "
                             "connection with salts is absent.")
            self.new_id = 0
        return self.new_id

    def salts_store_test_result(self, **kwargs):
        self.log.info("Saving results to salts... ")
        res = self.salts_api_client.save_test_result(**kwargs)
        self.log.info("Test results saved to salts")
        self.salts_store_artifact(res, "yt_conf", "lunapark_*.lock",
                                  self.lock_dir)
        return res.get('id')

    def salts_store_artifact(self, tr, field, templ, result_dir):
        file_path_g = "%s/%s" % (result_dir.rstrip("/"), templ)
        pathes = glob.glob(file_path_g)
        if not pathes:
            self.log.warning("Incorrect template: %s" % file_path_g)
            return
        file_path = pathes[0]
        if file_path:
            if not tr[field]:
                size = os.stat(file_path).st_size
                if size < SaltsPlugin.MAX_FILE_SIZE:
                    self.log.info("Saving file '{field}': {path}".format(
                        field=field, path=file_path))
                    tr_data = {field: file_path}
                    self.salts_api_client.update_test_result(tr["id"],
                                                             **tr_data)
                else:
                    self.log.error("File '{field}' too big - {size} byte(s), "
                                   "MAX = {max}".format(
                                       field=field,
                                       size=size,
                                       max=SaltsPlugin.MAX_FILE_SIZE))
            else:
                self.log.warn("File '{field}' already exist: {value}".format(
                    field=field, value=tr[0][field]))
        else:
            self.log.info("File '{field}' not found: {path}".format(
                field=field, path=file_path_g))

    def signal_handler(self, signal, frame):
        self.shooting_status = 'I'
        dt = None
        if self.current_data.get('start_time'):
            dt = datetime.datetime.now() \
                - self.current_data['start_time']['datetime']
        if dt and dt.seconds > SaltsPlugin.DB_SAVE_LOW_DURATION_BOUND:
            self.is_result_save = True
            self.log.info("Test duration is greater than %s "
                          "seconds. Test results will be "
                          "saved to DB." \
                          % SaltsPlugin.DB_SAVE_LOW_DURATION_BOUND)
            return self.yatank_signal_handler(signal, frame)
        print("Тест был прерван. Сохранить результаты тестов? [Y/N]: _   ")
        while True:
            s = sys.stdin.readline()
            if s in ["Y\n", "y\n"]:
                self.is_result_save = True
                break
            if s in ["N\n", "n\n"]:
                self.is_result_save = False
                break
            print("Выберите ответ ... [Y/N] _   ")
        if self.is_result_save:
            self.log.info("Test duration is less than %s "
                          "seconds but test results will "
                          "be saved to DB." \
                          % SaltsPlugin.DB_SAVE_LOW_DURATION_BOUND)
        else:
            self.log.info("Test duration is less than %s"
                          "seconds and test results won't "
                          "be saved to DB." \
                          % SaltsPlugin.DB_SAVE_LOW_DURATION_BOUND)
        return self.yatank_signal_handler(signal, frame)
