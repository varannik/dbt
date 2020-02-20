from django.urls import path
from  profiles_api import views
from django.urls import path, re_path



urlpatterns = [
    path('hello-view/',views.HelloAPIView.as_view()), 
]