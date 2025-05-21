from django.db import models

class Order(models.Model):
    id = models.BigAutoField(primary_key=True)
    cp_awb = models.CharField(max_length=100, blank=False, null=False, unique=True)
    updated_at = models.DateTimeField(auto_now=True)
    drop_info_latitude = models.DecimalField(blank=True, null=True, max_digits=20, decimal_places=15)
    drop_info_longitude = models.DecimalField(blank=True, null=True, max_digits=20, decimal_places=15)
    pickup_info_latitude = models.DecimalField(blank=True, null=True, max_digits=20, decimal_places=15)
    pickup_info_longitude = models.DecimalField(blank=True, null=True, max_digits=20, decimal_places=15)
    lat_long_accuracy_level = models.IntegerField(null=True, blank=True)
    delivery_type = models.CharField(max_length=50, blank=False, null=False)

    class Meta:
        db_table = 'order_order'
        managed = False
        app_label = 'geo_updater'