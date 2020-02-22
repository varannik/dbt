from rest_framework import serializers

class hello_serializer (serializers.Serializer):
    """"serilizer for testin APi view """
    name = serializers.CharField(max_length = 10)
    
    