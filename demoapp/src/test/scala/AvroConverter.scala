import org.apache.avro.Schema
import org.scalatest.FlatSpecLike
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

/**
  * Created by yoelamram on 9/30/16.
  */
class AvroConverter extends FlatSpecLike {

  val defaultSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |  "name": "AuditEvent",
      |  "type": "record",
      |  "fields": [
      |    {"name": "resource_owner", "type": ["null", "string"], "default": null},
      |    {"name": "trip_id", "type": ["null", "string"], "default": null},
      |    {"name": "additional_info_dict", "type": ["null", "string"], "default": null},
      |    {"name": "http_response", "type": ["null", "string"], "default": null},
      |    {"name": "device_id", "type": ["null", "string"], "default": null},
      |    {"name": "supplier_id", "type": ["null", "string"], "default": null},
      |    {"name": "resource_type", "type": ["null", "string"], "default": null},
      |    {"name": "elapsed", "type": ["null", "int"], "default": null},
      |    {"name": "server_fqdn", "type": ["null", "string"], "default": null},
      |    {"name": "ts", "type": ["null", "string"], "default": null},
      |    {"name": "user_id", "type": ["null", "string" ], "default": null},
      |    {"name": "uuid", "type": ["null", "string"], "default": null},
      |    {"name": "result", "type": ["null", "string"], "default": null},
      |    {"name": "suffix", "type": ["null", "string"], "default": null},
      |    {"name": "filename", "type": ["null", "string"], "default": null},
      |    {"name": "filepath", "type": ["null", "string"], "default": null},
      |    {"name": "fqn", "type": ["null", "string"], "default": null}
      |  ]
      |}
    """.stripMargin)

  val event =
    """
      |{"server_fqdn":"worker-13.prd.eu-west-1.karhoo.com","ts":"2016-09-28 06:57:12","supplier_id":"skyex","elapsed":120,"uuid":"c82df0ae-8548-11e6-8d15-02e0e41bf921","user_id":81950,"resource_owner":"torsion","resource_type":"availability","additional_info_dict":"{\"request\":{\"__sdo__\":\"core.sdo.models.Request\",\"id\":\"f4c94bbc-f1bc-496b-8f76-bad23992e4d8\",\"created\":{\"__dt__\":\"2016-09-28T06:57:12.209260+00:00\"},\"user_id\":81950},\"task_created_ts\":\"2016-09-28T06:57:12.273741+00:00\",\"torsion_method\":\"read\",\"torsion_type\":\"availability\",\"supplier\":\"skyex\",\"pre_queue_request_date\":\"2016-09-28T06:57:12.209260+00:00\",\"task\":{\"request\":{\"__sdo__\":\"core.sdo.models.Request\",\"created\":{\"__dt__\":\"2016-09-28T06:57:12.209260+00:00\"},\"id\":\"f4c94bbc-f1bc-496b-8f76-bad23992e4d8\",\"user_id\":81950},\"valid_suppliers\":[\"skyex\"],\"location\":{\"short_name_line1\":\"117 Chatham Road\",\"address\":{\"region\":null,\"zip_code\":\"SW11 6HJ\",\"country\":\"GB\",\"formatted_address\":\"117 Chatham Road, LONDON, SW11 6HJ, UNITED KINGDOM\",\"street\":\"Chatham Road\",\"line_1\":\"117 Chatham Road\",\"city\":\"London\",\"town\":null,\"__sdo__\":\"core.sdo.models.Address\",\"number\":\"117\",\"line_2\":\"Wandsworth\"},\"short_name_line2\":\"Wandsworth\",\"long_name\":\"117 Chatham Road, LONDON, SW11 6HJ, UNITED KINGDOM\",\"__sdo__\":\"core.sdo.models.Location\",\"latitude\":51.4554972,\"is_final\":true,\"tz\":null,\"type\":\"search\",\"place_id\":\"p_GB|RM|A|23942706\",\"longitude\":-0.1632424,\"airport\":null}},\"platform\":null}","result":"OK"}
    """.stripMargin

  "" should "" in {

    val converter = new JsonAvroConverter()
    val record = converter.convertToGenericDataRecord(event.getBytes, defaultSchema)
    println(record.toString)
  }
}
