from rest_framework.views import  APIView
from rest_framework import viewsets
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
    
    def put(self, request, pk = None):
        """Handle updating an object"""
        return Response({'method:':'PUT'})
    
    def patch (self,request,pk=None):
        """Handle patch and update"""
        return Response({'method:':'PATCH'})
    
    def delete(self,request,pk= None):
        """Handle delete method"""
        return Response({'method:':'DELETE'})


class HelloViewSet(viewsets.ViewSet):
    """Test Api ViewSets"""

    serializer_class = serializer.hello_serializer

    def list(self,request):
        """return a hello message"""
        a_viewset = ['Uses action ','akjshajksh']
        
        return Response({'message':'Hello','a viewSet:':a_viewset})

    def create (self,request):
        """Create a new Hello message"""
        serializer = self.serializer_class(data= request.data)

        if serializer.is_valid():
            name = serializer.validated_data.get('name')
            message = f'hello {name}'
            return Response ({'message': message})
        else: 
            return Response(
                serializer.errors,
                status= status.HTTP_400_BAD_REQUEST
            )

    def retrieve(self, request,pk = None):
        """ Handle getting an item """
        return Response({'http:': 'GET'})
    
    def update(self, request,pk = None):
        """ Handle updating an object """
        return Response({'http':'UPDATE'})

    def partial_update(self,request,pk = None):
        """handle partial update of an item """
        return Response({'http':'PARTIAL PATCH'})

    def destroy(self,request,pk=None):
        """Handle removing an item"""
        return Response({'http': 'DELETE'})





        

    