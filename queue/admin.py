# -*- coding: utf-8 -*-
from django.contrib import admin
from .models import Queue


class QueueAdmin(admin.ModelAdmin):
    pass

admin.site.register(Queue, QueueAdmin)
