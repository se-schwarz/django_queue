# -*- coding: utf-8 -*-
import logging
import datetime
from django.contrib.contenttypes.generic import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.db import models
import pytz
import sys

logger = logging.getLogger(__name__)


class QueueManager(models.Manager):
    timezone = pytz.timezone('Europe/Berlin')

    def enqueue(self, *instances, **kwargs ):
        """
        Put the given instances into the queue.

        If an object is already in the queue, delete the old one and put a new
        object into the queue.
        """
        deleted = kwargs.pop('deleted', False)
        due = kwargs.pop('due', datetime.datetime.now(self.timezone))
        function = kwargs.pop('function')
        instance_by_ct = {}

        # Group instances by content type.
        for instance in instances:
            cls = instance.__class__
            content_type = ContentType.objects.get_for_model(cls)
            if content_type not in instance_by_ct:
                instance_by_ct[content_type] = []
            instance_by_ct[content_type].append(instance)

        for content_type, instances in instance_by_ct.iteritems():

            # Let's see what objects are already enqueued.
            instance_pks = [instance.pk for instance in instances]
            existing_queue = self.filter(
                content_type=content_type,
                object_id__in=instance_pks)
            # Delete the ones that are already in the queue. Therefore we
            # prevent indexing a object that got updated multiple times
            # recently to be indexed more than once.
            existing_queue.delete()

            # We delete the old ones instead of not creating the new one for
            # one particular reason:
            # It can happen that an object is already beeing processed just
            # now. If we now skip the creation, the new version won't get
            # indexed since the ``process_search_queue`` command only took care
            # of the OLD version.

            to_be_created = [
                self.model(
                    content_object=instance,
                    deleted=deleted,
                    due=due,
                    function=function
                )
                for instance in instances]
            self.bulk_create(to_be_created)

    def process(self, queryset=None, max_execution_time=datetime.timedelta(seconds=300)):
        current_time = datetime.datetime.now(self.timezone)

        if queryset is None:
            queryset = self.filter(
                due__lte=current_time,
                executed__isnull=True,
            ).order_by('due')

        for instance in queryset:
            instance.process()
            if max_execution_time is not None and \
               datetime.datetime.now(self.timezone) >= current_time + max_execution_time:
                break


class Queue(models.Model):
    """
        Save your objects and the required function into this model and
        let the queue call it at at the required due date
    """
    content_type = models.ForeignKey(ContentType)
    object_id = models.IntegerField()
    content_object = GenericForeignKey('content_type', 'object_id')
    function = models.CharField(max_length=255)

    deleted = models.BooleanField(default=False)

    created = models.DateTimeField(auto_now_add=True)
    due = models.DateTimeField()
    executed = models.DateTimeField(null=True, blank=True)

    objects = QueueManager()

    def process(self):
        model = self.content_type.model_class()
        if not self.deleted and self.content_object is not None:
            obj = self.content_object
        else:
            obj = model(pk=self.object_id)
        try:
            getattr(obj, self.function)()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.error(
                u'(Queue #%d) Error processing (%s / pk=%d): %s (in %s / Line: %s)' % (
                    self.pk,
                    obj.__class__.__name__,
                    obj.pk,
                    e,
                    exc_traceback.tb_frame.f_code.co_filename,
                    exc_traceback.tb_lineno
                )
            )
        finally:
            self.executed = datetime.datetime.now(pytz.timezone('Europe/Berlin'))
            self.save()












