package UDAF;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


//这个还不会使用
public class MyUDAFTest extends UserDefinedAggregateFunction {
    private StructType inputSchema;
    private StructType bufferSchema;

    //1、该聚合函数的输入参数的数据类型
    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    //2、聚合缓冲区中的数据类型.（有序性）
    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }
    //3、返回值的数据类型
    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    //4、这个函数是否总是在相同的输入上返回相同的输出,一般为true
    @Override
    public boolean deterministic() {
        return true;
    }

    //5、初始化给定的聚合缓冲区,在索引值为0的sum=0;索引值为1的count=1;
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,0D);
        buffer.update(1,0D);
    }
    //6、更新
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        //如果input的索引值为0的值不为0
        if(!input.isNullAt(0)){
            double updateSum = buffer.getDouble(0) + input.getDouble(0);
            double updateCount = buffer.getDouble(1) + 1;
            buffer.update(0,updateSum);
            buffer.update(1,updateCount);
        }
    }
    //7、合并两个聚合缓冲区,并将更新后的缓冲区值存储回“buffer1”
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        double mergeSum = buffer1.getDouble(0) + buffer2.getDouble(0);
        double mergeCount = buffer1.getDouble(1) + buffer2.getDouble(1);
        buffer1.update(0,mergeSum);
        buffer1.update(1,mergeCount);
    }

    //8、计算出最终结果
    public Double evaluate(Row buffer) {
        return buffer.getDouble(0)/buffer.getDouble(1);
    }
}
