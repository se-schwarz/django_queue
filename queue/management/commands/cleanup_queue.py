# -*- coding: utf-8 -*-
from optparse import make_option
import datetime
from django.core.management import BaseCommand
import pytz
from queue.models import Queue


class Command(BaseCommand):
    help = 'Cleanup executed queue entries. '

    option_list = BaseCommand.option_list + (
        make_option('--all', action='store_true', dest='all', default=False,
                    help='Cleanup all executed queue items.'),
        make_option('--threshold', action='store', dest='threshold', default=False,
                    help='Cleanup all executed queue items that are older than current time + threshold in seconds '),
    )

    def cleanup_all(self):
        qs = Queue.objects.filter(executed__isnull=False)
        print "Deleting {} executed queue item(s)...".format(qs.count())
        qs.delete()

    def cleanup(self, threshold):
        qs = Queue.objects.filter(
            executed__lte=datetime.datetime.now(pytz.timezone('Europe/Berlin'))+threshold
        )
        print "Deleting {} executed queue item(s)...".format(qs.count())
        qs.delete()

    def handle(self, *args, **options):
        if options['all']:
            self.cleanup_all()
            return
        if options['threshold']:
            self.cleanup(datetime.timedelta(seconds=int(options['threshold'])))
            return
