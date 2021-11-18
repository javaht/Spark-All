package UDF;


import org.apache.spark.sql.api.java.UDF1;

public class MyUdf implements UDF1<String,String> {



    @Override
    public String call(String name) throws Exception {
    String Myname = name.substring(0,1);

         if(Myname.equals("李")){

             return name;
         }else{
             name = "";
             return name;
         }


    }
}

