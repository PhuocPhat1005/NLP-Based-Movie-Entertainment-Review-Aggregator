from django.urls import path
from .views import FilmListAPIView, ReviewsAPIView

urlpatterns = [
    path('films', FilmListAPIView.as_view(), name='film-list'),
    path('reviews', ReviewsAPIView.as_view(), name='reviews'),
]