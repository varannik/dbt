from rest_framework.views import  APIView
from rest_framework.response import Response
from rest_framework import status
from profiles_api import serializer

class HelloAPIView(APIView):
    """Test API view """

    serializer_class = serializer.hello_serializer

    def get(self,request,format = None):
        """return  a rest of features"""
        an_view = ["mother fucker "]

        return Response({'message':'hello','API view':an_view})

    def post (self, request):
        """"Create a Hello message with our name"""
        serializer = self.serializer_class(data= request.data)
        
        if serializer.is_valid():
            name = serializer.validated_data.get('name')
            message = f'hello {name}'
            return Response({'message':message})
        else:
          return Response(serializer.errors, status= status.HTTP_400_BAD_REQUEST )


        
        

    