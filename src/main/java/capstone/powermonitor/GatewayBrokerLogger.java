package capstone.powermonitor;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.eclipse.kura.configuration.ConfigurableComponent;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class GatewayBrokerLogger implements ConfigurableComponent, MqttCallback {	
	private static final Logger s_logger = LoggerFactory.getLogger(GatewayBrokerLogger.class);
	
	// Cloud Application identifier
	private static final String APP_ID = "GatewayBrokerLogger";

	// Publishing Property Names
	private static final String   MQTT_TOPIC_PROP_NAME   = "logging.mqttTopic";
		
	private MqttClient 					mqttClient;
	
	private Map<String, Object>         m_properties;
	private String 						broker;
	private String						topic;
	
	
	// ----------------------------------------------------------------
	//
	//   Dependencies
	//
	// ----------------------------------------------------------------
	
	public GatewayBrokerLogger() 
	{
		super();
	}
		
	// ----------------------------------------------------------------
	//
	//   Activation APIs
	//
	// ----------------------------------------------------------------

	protected void activate(ComponentContext componentContext, Map<String,Object> properties) 
	{
		s_logger.info("Activating " + APP_ID + "...");
		
		m_properties = properties;
		for (String s : properties.keySet()) {
			s_logger.info("Activate - "+s+": "+properties.get(s));
		}
		
		topic = (String) m_properties.get(MQTT_TOPIC_PROP_NAME);
		broker = "tcp://127.0.0.1:1883";
		
		// get the mqtt client for this application
		s_logger.info("Connecting MqttClient for {}...", APP_ID);
		try {
	        mqttClient = new MqttClient(broker, APP_ID);
	        mqttClient.connect();
	        mqttClient.setCallback(this);
	        
			s_logger.info("subscribe mqtt client to: " + topic);
	        mqttClient.subscribe(topic);
	    } catch (MqttException e) {
	        e.printStackTrace();
	    }
		s_logger.info("Activating " + APP_ID + " ... Done.");
	}
	
	
	protected void deactivate(ComponentContext componentContext) 
	{
		s_logger.debug("Deactivating " + APP_ID + "...");
		
		// Releasing the CloudApplicationClient
		s_logger.info("Releasing MqttClient for {}...", APP_ID);
		try {
			mqttClient.disconnect();
		} catch (MqttException e) {
			e.printStackTrace();
		}

		s_logger.debug("Deactivating " + APP_ID + "... Done.");
	}	
	
	
	public void updated(Map<String,Object> properties)
	{
		s_logger.info("Updated " + APP_ID + "...");

		try {
			s_logger.info("unsubscribe mqtt client from: " + topic);
			mqttClient.unsubscribe(topic);
		} catch (MqttException e) {
			e.printStackTrace();
		}
		
		// store the properties received
		m_properties = properties;
		for (String s : properties.keySet()) {
			s_logger.info("Update - "+s+": "+properties.get(s));
		}
		
		topic = (String) m_properties.get(MQTT_TOPIC_PROP_NAME);

		try {
			s_logger.info("subscribe mqtt client to: " + topic);
			mqttClient.subscribe(topic);
		} catch (MqttException e) {
			e.printStackTrace();
		}
		
		
		
		s_logger.info("Updated " + APP_ID + "... Done.");
	}
	
	
	
	// ----------------------------------------------------------------
	//
	//   MQTT Paho Application Callback Methods
	//
	// ----------------------------------------------------------------
	
	@Override
	public void connectionLost(Throwable cause) {}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		s_logger.info("Recieved MQTT -- Topic: "+ topic +" Message: " + message);   
		try{		
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			StringReader reader = new StringReader(message.toString());
	        XMLStreamReader streamReader = inputFactory.createXMLStreamReader(reader);
	        
	        //<payload>
	        //	<metrics>
	        //		<metric>
	        //			<name>Power</name>
	        //			<type>double</type>
	        //			<value>3.0347696184267443</value>
	        //		</metric>
	        //	</metrics>
	        //</payload>
	//        KuraPayload payload = new KuraPayload();
	        Map<String,Object> metrics =  new HashMap<String,Object>();
	                
	        streamReader.nextTag(); // Advance to "payload" element
	        streamReader.nextTag(); // Advance to "metrics" element
	        streamReader.nextTag(); // Advance to "metric" element
	
	        int metricsNumber = 0;
	       	String name = new String();
	    	String typename = new String();
	    	String value = new String();
	        while (streamReader.hasNext()) {
	        	//Checking Start of Element
	            if (streamReader.isStartElement()) {
	                switch (streamReader.getLocalName()) {
		                case "name": {
		                	name = streamReader.getElementText();
		                    break;
		                }
		                case "type": {
		                	typename = streamReader.getElementText();
		                	break;
		                }
		                case "value": {
		                	value = streamReader.getElementText();
		                    break;
		                }
		                case "metric" : {
		                	metricsNumber ++;
		                	break;
		                }
	                }
	            }
	        	//Checking End of Element
	            if(streamReader.isEndElement()){
	            	switch (streamReader.getLocalName()) {
		                case "metric" : {
	
		                	//Dont know a better way to do this...
		                	//valid metric types: string, double, int, float, long, boolean, base64Binary
		                    switch (typename) {
		                    	case "string": {
		                            metrics.put(name, (String)value);
		                    		break;
		                        }
		                    	case "double": {
		                            metrics.put(name, Double.parseDouble(value));
		                            break;
		                        }
		                    	case "int": {
		                            metrics.put(name, Integer.parseInt(value));
		                    		break;
		                        }
		                    	case "float": {
		                            metrics.put(name, Float.parseFloat(value));
		                            break;
		                        }
		                    	case "long": {
		                            metrics.put(name, Long.parseLong(value));
		                            break;
		                        }
		                    	case "boolean": {
		                            metrics.put(name, Boolean.parseBoolean(value));
		                    		break;
		                        }
		                    	case "base64Binary": {
		                        	//not sure here
		                            break;
		                    	}
		                    }
		                    break;
		                }
	            	}
	            }
	            streamReader.next();
	        }
	        
	        s_logger.info(metricsNumber + " metrics");
	        s_logger.info("Metrics: " + metrics.toString());
		}
		catch(Exception e){
			s_logger.info(e.toString());
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {}
	
	
	// ----------------------------------------------------------------
	//
	//   Private Methods
	//
	// ----------------------------------------------------------------
	 
}