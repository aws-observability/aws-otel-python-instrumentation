from django.db import models


class Vehicle(models.Model):
    id = models.AutoField(primary_key=True)
    make = models.CharField(max_length=255)
    model = models.CharField(max_length=255)
    year = models.IntegerField()
    image_name = models.TextField(max_length=255)
