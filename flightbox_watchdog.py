#!/usr/bin/env python3

"""flightbox_watchdog.py: Script that checks if required FlightBox and OGN processes are running and (re-)starts them if required. Can be used to start and monitor FlightBox via a cronjob."""

from os import path
import psutil
from utils.detached_screen import DetachedScreen
import time

__author__ = "Thorsten Biermann"
__copyright__ = "Copyright 2015, Thorsten Biermann"
__email__ = "thorsten.biermann@gmail.com"


# define flightbox processes that must be running
required_flightbox_processes = {}
required_flightbox_processes['flightbox'] = {'status': None}
required_flightbox_processes['flightbox_datahubworker'] = {'status': None}
required_flightbox_processes['flightbox_output_network_airconnect'] = {'status': None}
required_flightbox_processes['flightbox_transformation_sbs1ognnmea_flarm'] = {'status': None}
required_flightbox_processes['flightbox_input_network_sbs1'] = {'status': None}
required_flightbox_processes['flightbox_input_network_softrf'] = {'status': None}
# required_flightbox_processes['flightbox_input_network_ogn_server'] = {'status': None}
# required_flightbox_processes['flightbox_input_serial_gnss'] = {'status': None}

# define command for starting flightbox
flightbox_command = '/home/pi/flightbox/flightbox.py'

# define OGN processes that must be running
required_ogn_processes = {}
# required_ogn_processes['ogn-rf'] = {'status': None}
# required_ogn_processes['ogn-decode'] = {'status': None}

# define path where OGN binaries are located
ogn_path = '/home/pi/rtlsdr-ogn'


def check_flightbox_processes():
    global required_flightbox_processes

    for p in psutil.process_iter():
        if p.name() in required_flightbox_processes.keys():
            required_flightbox_processes[p.name()]['status'] = p.status()


def kill_all_flightbox_processes():
    for p in psutil.process_iter():
        if p.name().startswith('flightbox'):
            print("Killing process {}".format(p.name()))
            p.kill()


def start_flightbox():
    global flightbox_command

    print("Starting flightbox inside screen")
    s = DetachedScreen('flightbox', command=flightbox_command, initialize=True)
    s.disable_logs()


def restart_flightbox():
    kill_all_flightbox_processes()
    time.sleep(1.0)
    start_flightbox()


def check_ogn_processes():
    global required_ogn_processes

    for p in psutil.process_iter():
        if p.name() in required_ogn_processes.keys():
            required_ogn_processes[p.name()]['status'] = p.status()


def kill_all_ogn_processes():
    global ogn_path

 #   for p in psutil.process_iter():
 #       if p.name().startswith('ogn'):
 #           print("Killing process {}".format(p.name()))
 #           p.kill()


def start_ogn():
    global ogn_path

    print("Starting OGN inside screen")

#    s_rf = DetachedScreen('ogn_rf', command="{} {}".format(path.join(ogn_path, 'ogn-rf'), path.join(ogn_path, 'ogn.conf')), initialize=True)
#    s_rf.disable_logs()

#    s_decode = DetachedScreen('ogn_decode', command="{} {}".format(path.join(ogn_path, 'ogn-decode'), path.join(ogn_path, 'ogn.conf')), initialize=True)
#    s_decode.disable_logs()


def restart_ogn():
    kill_all_ogn_processes()
    time.sleep(1.0)
    start_ogn()


# check if script is executed directly
if __name__ == "__main__":
    check_flightbox_processes()
    check_ogn_processes()

    is_flightbox_restart_required = False
    is_ogn_restart_required = False

    for p in required_flightbox_processes.keys():
        if required_flightbox_processes[p]['status'] not in ['running', 'sleeping']:
            print("{} not running".format(p))
            is_flightbox_restart_required = True
#            is_ogn_restart_required = True

#    for p in required_ogn_processes.keys():
#        if required_ogn_processes[p]['status'] not in ['running', 'sleeping']:
#            print("{} not running".format(p))
#            is_ogn_restart_required = True

    if is_flightbox_restart_required:
        print('== Restarting FlightBox')
        restart_flightbox()

 #   if is_ogn_restart_required:
 #       time.sleep(10.0)
 #       print('== Restarting OGN')
 #       restart_ogn()
