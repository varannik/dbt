from rest_framework.views import  APIView
from rest_framework.response import Response


class HelloAPIView(APIView):
    """Test API view """
    def get(self,request,format = None):
        """return  a rest of features"""
        an_view = ["mother fucker "]
        return Response({'message':'hello','API view':an_view})

