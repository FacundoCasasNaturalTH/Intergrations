// camel-k: language=java
// camel-k: dependency=camel:jackson
// camel-k: dependency=mvn:com.danone:models:1.0.5

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.model.dataformat.JsonLibrary;

import com.danone.asnConfirmation.input.InputASNConfirmation;
import com.danone.asnConfirmation.output.LineAsnConfirmation;
import com.danone.asnConfirmation.output.OutputAsnConfirmation;

import com.danone.asn.input.Detail;
import com.danone.asn.input.InputASN;
import com.danone.asn.input.Line;
import com.danone.asn.output.OutputASN;

import com.danone.deliveryInstruction.input.InputDeliveryInstruction;
import com.danone.deliveryInstruction.input.OrderLine;
import com.danone.deliveryInstruction.output.OutputDeliveryInstruction;

import org.apache.camel.builder.RouteBuilder;

public class DanoneIntegration extends RouteBuilder {

  @Override
  public void configure() throws Exception {

    rest()
        .post("/asn")
        .responseMessage(200, "post endpoint")
        .description("Post Endpoint")
        .type(InputASN.class)
        .consumes("application/json")
        .produces("application/json")
        .to("direct:asnRoute");

    from("direct:asnRoute")
        .unmarshal().json(JsonLibrary.Jackson, InputASN.class)
        .process(new ProcessHandleASN())
        .marshal().json()
        .setHeader(Exchange.HTTP_METHOD, constant("POST"))
        .toD("http://app-server.poc-camelk-p-s.svc.cluster.local/execute?bridgeEndpoint=true&throwExceptionOnFailure=false")
        .log("{$body}");

    rest()
        .post("/asn/confirmation")
        .responseMessage(200, "post endpoint")
        .description("Post Endpoint")
        .type(InputASNConfirmation[].class)
        .consumes("application/json")
        .produces("application/json")
        .to("direct:demo");

    from("direct:demo")
        .unmarshal().json(JsonLibrary.Jackson, InputASNConfirmation[].class)
        .process(new ProcessHandleAsnConfirmation())
        .marshal().json()
        .setHeader(Exchange.HTTP_METHOD, constant("POST"))
        .toD("http://app-server.poc-camelk-p-s.svc.cluster.local/execute?bridgeEndpoint=true&throwExceptionOnFailure=false")
        .log("{$body}");

    rest()
        .post("/deliveryInstruction")
        .responseMessage(200, "post endpoint")
        .description("Post Endpoint")
        .type(InputDeliveryInstruction.class)
        .consumes("application/json")
        .produces("application/json")
        .to("direct:deliveryInstructionRoute")

    ;

    from("direct:deliveryInstructionRoute")
        .unmarshal().json(JsonLibrary.Jackson, InputDeliveryInstruction.class)
        .process(new ProcessHandleDeliveryInstruction())
        .marshal().json()
        .setHeader(Exchange.HTTP_METHOD, constant("POST"))
        .toD("http://app-server.poc-camelk-p-s.svc.cluster.local/execute?bridgeEndpoint=true&throwExceptionOnFailure=false")
        .log("{$body}");
  }

}

class ProcessHandleAsnConfirmation implements Processor {

  @Override
  public void process(Exchange exchange) throws Exception {
    List<InputASNConfirmation> body = (List<InputASNConfirmation>) exchange.getMessage().getBody(List.class);
    OutputAsnConfirmation output = new OutputAsnConfirmation();
    List<LineAsnConfirmation> lines = new ArrayList<>();

    output.setDocumentDate(body.stream().findFirst().get().getBLDAT());
    output.setDeliveryNumber(body.stream().findFirst().get().getXBLNR());
    output.setInternalOrderID(body.stream().findFirst().get().getBKTXT());
    output.setPurchasingDocumentNumber(body.stream().findFirst().get().getEBELN());
    output.setPostingDate(body.stream().findFirst().get().getBUDAT());
    output.setPlantCode(body.stream().findFirst().get().getWERKS());

    for (InputASNConfirmation inputASN : body) {
      LineAsnConfirmation line = new LineAsnConfirmation();

      line.setQuantity(inputASN.getERFMG());
      line.setUnitOfMeasure(inputASN.getERFME());
      line.setStorageLocation(inputASN.getLGORT());
      line.setBatch(inputASN.getCHARG());
      line.setVendorBatchNumber(inputASN.getLICHA());
      line.setBatchProductionDate(inputASN.getHSDAT());

      lines.add(line);

    }
    output.setLine(lines);

    exchange.getMessage().setBody(output);
    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
  }

}

class ProcessHandleDeliveryInstruction implements Processor {

  @Override
  public void process(Exchange exchange) throws Exception {
    InputDeliveryInstruction body = exchange.getMessage().getBody(InputDeliveryInstruction.class);
    List<OutputDeliveryInstruction> output = new ArrayList<>();

    for (OrderLine line : body.getOrderLine()) {

      OutputDeliveryInstruction outputDelivery = new OutputDeliveryInstruction();
      outputDelivery.setFecha(null);
      outputDelivery.setGuia(body.getDeliveryNumber());
      outputDelivery.setFletero(body.getDeliveryCustomerNumber());
      outputDelivery.setReparto(null);
      outputDelivery.setCliente(body.getDeliveryCustomerNumber());
      outputDelivery.setAlmacen(body.getPlantCode());
      outputDelivery.setProducto(line.getProductNumber());
      outputDelivery.setCantidadPedida(line.getQtyInBasicUnit());
      outputDelivery.setBaseUnit(line.getBaseUnit());
      outputDelivery.setQtyInDeliveryUnit(line.getQtyInDeliveryUnit());
      outputDelivery.setDeliveryUnit(line.getDeliveryUnit());

      output.add(outputDelivery);

    }
    exchange.getMessage().setBody(output);
    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
  }

}

class ProcessHandleASN implements Processor {

  @Override
  public void process(Exchange exchange) throws Exception {
    InputASN body = exchange.getMessage().getBody(InputASN.class);
    List<OutputASN> output = new ArrayList<>();

    for (Line line : body.getLine()) {

      for (Detail detail : line.getDetail()) {
        OutputASN outputASN = new OutputASN();
        outputASN.setFieldChr1(body.getDeliveryNumber());
        outputASN.setFecRemito(body.getPlannedDeliveryDate());
        outputASN.setCodAlmacenOrigen(body.getShipFromLocation());
        outputASN.setCodAlmacen(body.getShipFromLocation());
        outputASN.setFieldChr2(line.getPurchaseOrderNumber());
        outputASN.setFieldChr3(line.getProductNumber());
        outputASN.setCanRemito(line.getQuantityInBasicUnit());
        outputASN.setUSAP(line.getBasicUnit());
        outputASN.setFecVencimiento(detail.getBatch());
        outputASN.setSSAP(line.getStockType());
        outputASN.setNroSscc(detail.getSscc());

        output.add(outputASN);
      }

    }
    exchange.getMessage().setBody(output);
    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
  }

}
