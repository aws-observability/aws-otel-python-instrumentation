# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from django.db import models


class Vehicle(models.Model):
    id = models.AutoField(primary_key=True)
    make = models.CharField(max_length=255)
    model = models.CharField(max_length=255)
    year = models.IntegerField()
    image_name = models.TextField(max_length=255)


class VehiclePurchaseHistory(models.Model):
    id = models.AutoField(primary_key=True)
    vehicle = models.ForeignKey("Vehicle", on_delete=models.CASCADE)
    purchase_date = models.DateField(auto_now_add=True)
    purchase_price = models.IntegerField()
