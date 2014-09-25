# -*- coding: utf-8 -*-

'''
Usage::

    flock -w 10 /path/to/index.lock -c "/path/to/local/python /path/to/manage.py process_queue"
'''
import time
import datetime
from optparse import make_option
from django.core.management import BaseCommand
from queue.models import Queue


class Command(BaseCommand):
    help = 'Processes all queued tasks. This command runs for the given, time, polling ' \
           'every few seconds for new items in the queue.'

    option_list = BaseCommand.option_list + (
        make_option('--once', action='store_true', dest='once', default=False,
                    help='Empty queue once and then return.'),
        make_option('--execution_time', action='store', dest='execution_time', default=False,
                    help='The process will stop after this time in seconds and '
                         'after the currently running iteration finished.'),
        make_option('--polling_interval_time', action='store', dest='polling_interval_time', default=False,
                    help='Polling interval in seconds.'),
    )

    execution_time = 60 * 5
    interval = 4

    def run(self):
        print "Processing of the queue started..."
        for i in xrange(self.execution_time / self.interval - 1):
            Queue.objects.process(max_execution_time=datetime.timedelta(seconds=self.execution_time/2))
            time.sleep(self.interval)

    def process_all(self):
        print "Processing all objects in the queue..."
        Queue.objects.process(max_execution_time=None)

    def handle(self, *args, **options):
        if options['execution_time']:
            self.execution_time = int(options['execution_time'])
        if options['polling_interval_time']:
            self.interval = int(options['polling_interval_time'])

        if options['once']:
            self.process_all()
        else:
            self.run()







