from django.db import models
from django.contrib.auth.models import User

class Vehicle(models.Model):
    id = models.AutoField(primary_key=True)
    make = models.CharField(max_length=255)
    model = models.CharField(max_length=255)
    year = models.IntegerField()
    imageName = models.TextField(max_length=255)
