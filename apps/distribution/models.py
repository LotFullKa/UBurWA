from django.db import models


class Data(models.Model):
    """pre-processed Data Base model."""

    data = models.FileField()
