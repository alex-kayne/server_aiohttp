from django.db import models
from datetime import datetime


class Users(models.Model):
    email = models.EmailField(db_index=True, unique=True, null=False, default='old@gmail.com')
    method = models.TextField(null=True)
    password = models.BinaryField(null=True)
    device_id = models.TextField(null=True)
    family = models.TextField(null=True, blank=True)
    dt_created = models.DateTimeField(null=True, blank=True)
    dt_updated = models.DateTimeField(null=True, blank=True)
    class Meta:
        db_table = 'users'


#class Sessions(models.Model):
#    email = models.ForeignKey(Users, db_index=True, on_delete=models.CASCADE)
#    session_id = models.TextField()
#    session_expiry_date = models.DateTimeField()
#    dt_created = models.DateTimeField(default=datetime.now())
#    dt_updated = models.DateTimeField(default=datetime.now())

#    class Meta:
#       db_table  = 'sessions'


