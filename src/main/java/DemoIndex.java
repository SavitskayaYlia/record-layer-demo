import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;

public class DemoIndex {

    public static void main(String[] args) {

        OrderIndex orderIndex = OrderIndex.getOrderIndex("record-layer-demo-index");
        orderIndex.Delete();
        orderIndex.AddMassege("name" ,1 , 11);
        orderIndex.AddMassege("name" ,1 , 22);
        orderIndex.AddMassege("name" ,1 , 33);
        orderIndex.AddMassege("name" ,2 , 33);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("OrderIndex")
                .setFilter( Query.field("order_id").equalsValue(1))
                .build();
        orderIndex.Exacute(query1);

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("OrderIndex")
                .setFilter( Query.and(
                        Query.field("order_id").equalsValue(1),
                        Query.field("sub_order_id").equalsValue(11)))
                .build();
        orderIndex.Exacute(query2);

        RecordQuery query3 = RecordQuery.newBuilder()
                .setRecordType("OrderIndex")
                .setFilter( Query.and(
                        Query.field("sub_order_id").equalsValue(1),
                        Query.field("order_id").equalsValue(11)))
                .build();
        orderIndex.Exacute(query3);

        RecordQuery query4 = RecordQuery.newBuilder()
                .setRecordType("OrderIndex")
                .setFilter( Query.field("sub_order_id").equalsValue(1) )
                .build();
        orderIndex.Exacute(query4);

        RecordQuery query5 = RecordQuery.newBuilder()
                .setRecordType("OrderIndex")
                .setFilter( Query.field("name").equalsValue("name") )
                .build();
        orderIndex.Exacute(query5);
    }
}
