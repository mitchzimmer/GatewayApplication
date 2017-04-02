package capstone.peakmonitor;

import java.io.StringReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudClientListener;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.ComponentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeakMonitor implements ConfigurableComponent, CloudClientListener, MqttCallback {	
	private static final Logger s_logger = LoggerFactory.getLogger(PeakMonitor.class);
	
	// Cloud Application identifier
	private static final String APP_ID = "PeakMonitor";

	private CloudService                m_cloudService;
	private CloudClient      			m_cloudClient;
	
	private MqttClient 					mqttClient;
	
	private ScheduledExecutorService    m_worker;
	private ScheduledFuture<?>          m_handle;
	
	// Publishing Property Names
	private static final String   PUBLISH_RATE_PROP_NAME = "publish.rate";
	private static final String   MQTT_TOPIC_PROP_NAME = "logging.mqttTopic";
		
	
	private Map<String, Object>         	m_properties;
	private String 							broker;
	private String							gatewayBrokerTopic;
	private HashMap<String, KuraPayload> 	latestPayloads;
	
	
	// ----------------------------------------------------------------
	//
	//   Dependencies
	//
	// ----------------------------------------------------------------
	
	public PeakMonitor() 
	{
		super();
		m_worker = Executors.newSingleThreadScheduledExecutor();
	}

	public void setCloudService(CloudService cloudService) {
		m_cloudService = cloudService;
	}

	public void unsetCloudService(CloudService cloudService) {
		m_cloudService = null;
	}
	
		
	// ----------------------------------------------------------------
	//
	//   Activation APIs
	//
	// ----------------------------------------------------------------

	protected void activate(ComponentContext componentContext, Map<String,Object> properties) 
	{
		s_logger.info("Activating " + APP_ID + "...");
		
		//getting properties
		m_properties = properties;
		for (String s : properties.keySet()) {
			s_logger.info("Activate - "+s+": "+properties.get(s));
		}
		
		//create new instance of latest payloads
		latestPayloads = new HashMap<String, KuraPayload>();
		
		// get the mqtt CloudApplicationClient for this application
		try  {
			
			// Acquire a Cloud Application Client for this Application 
			s_logger.info("Getting CloudClient for {}...", APP_ID);
			m_cloudClient = m_cloudService.newCloudClient(APP_ID);
			m_cloudClient.addCloudClientListener(this);
			
			// Don't subscribe because these are handled by the default 
			// subscriptions and we don't want to get messages twice			
			doUpdate(false);
		}
		catch (Exception e) {
			s_logger.error("Error during component activation", e);
			throw new ComponentException(e);
		}
		
		
		// get the mqtt GatewayBrokerClient for this application
		gatewayBrokerTopic = (String) m_properties.get(MQTT_TOPIC_PROP_NAME);
		broker = "tcp://127.0.0.1:1883";
		
		s_logger.info("Connecting MqttClient for {}...", APP_ID);
		try {
	        mqttClient = new MqttClient(broker, APP_ID);
	        mqttClient.connect();
	        mqttClient.setCallback(this);
	        
			s_logger.info("subscribe mqtt client to: " + gatewayBrokerTopic);
	        mqttClient.subscribe(gatewayBrokerTopic);
	    } catch (MqttException e) {
	        e.printStackTrace();
	    }
		
		s_logger.info("Activating " + APP_ID + " ... Done.");
	}
	
	
	protected void deactivate(ComponentContext componentContext) 
	{
		s_logger.debug("Deactivating " + APP_ID + "...");
		
		// shutting down the worker and cleaning up the properties
		m_worker.shutdown();
		
		// Releasing the CloudApplicationClient
		s_logger.info("Releasing CloudApplicationClient for {}...", APP_ID);
		m_cloudClient.release();
		
		// Releasing the GatewayBrokerClient
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
		
		//unsubscribe GatewayClientBroker
		try {
			s_logger.info("unsubscribe mqtt client from: " + gatewayBrokerTopic);
			mqttClient.unsubscribe(gatewayBrokerTopic);
		} catch (MqttException e) {
			e.printStackTrace();
		}
		
		// store the properties received
		m_properties = properties;
		for (String s : properties.keySet()) {
			s_logger.info("Update - "+s+": "+properties.get(s));
		}
		
		//Get new topic and subscribe GatewayClientBroker
		gatewayBrokerTopic = (String) m_properties.get(MQTT_TOPIC_PROP_NAME);

		try {
			s_logger.info("subscribe mqtt client to: " + gatewayBrokerTopic);
			mqttClient.subscribe(gatewayBrokerTopic);
		} catch (MqttException e) {
			e.printStackTrace();
		}
		
		// try to kick off a new job
		doUpdate(true);
		s_logger.info("Updated " + APP_ID + "... Done.");
	}
	
	
	
	// ----------------------------------------------------------------
	//
	//   Cloud Application Callback Methods
	//
	// ----------------------------------------------------------------
	
	@Override
	public void onControlMessageArrived(String deviceId, String appTopic, KuraPayload msg, int qos, boolean retain) {}

	@Override
	public void onMessageArrived(String deviceId, String appTopic, KuraPayload msg, int qos, boolean retain) {}

	@Override
	public void onConnectionLost() {}

	@Override
	public void onConnectionEstablished() {}

	@Override
	public void onMessageConfirmed(int messageId, String appTopic) {}

	@Override
	public void onMessagePublished(int messageId, String appTopic) {}
	
	// ----------------------------------------------------------------
	//
	//   MQTT Paho Application Callback Methods
	//
	// ----------------------------------------------------------------
	
	@Override
	public void connectionLost(Throwable cause) {}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		s_logger.info("Recieved MQTT -- Topic: "+ topic +" Message: " + message);   
		
		String[] topicFragments = topic.split("/");
		// topicFragments[0] == {appSetting.topic_prefix}
		// topicFragments[1] == {unique_id}
		
		parseMetrics(topicFragments[1], message);
	}
	
	// ----------------------------------------------------------------
	//
	//   Private Methods
	//
	// ----------------------------------------------------------------

	/**
	 * Called after a new set of properties has been configured on the service
	 */
	private void doUpdate(boolean onUpdate) 
	{	
		// cancel a current worker handle if one if active
		if (m_handle != null) {
			m_handle.cancel(true);
		}
		
		if (!m_properties.containsKey(PUBLISH_RATE_PROP_NAME)) {
			s_logger.info("Update " + APP_ID + " - Ignore as properties do not contain PUBLISH_RATE_PROP_NAME.");
			return;
		}
		
		// schedule a new worker based on the properties of the service
		int pubrate = (Integer) m_properties.get(PUBLISH_RATE_PROP_NAME);
		m_handle = m_worker.scheduleAtFixedRate(new Runnable() {		
			@Override
			public void run() {
				Thread.currentThread().setName(getClass().getSimpleName());
				doPublish();
				latestPayloads = new HashMap<String, KuraPayload>();
			}
		}, 0, pubrate, TimeUnit.MINUTES);
	}
	
	
	/**
	 * Called on Paho MQTT messageArrived to publish the next KuraPayload sent to cloud.
	 */
	
	private void parseMetrics(String topic, MqttMessage message){
		KuraPayload payload = new KuraPayload();
        payload.setTimestamp(new Date());
        
		//Parsing out XML into KuraPayload
		try{		
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			StringReader reader = new StringReader(message.toString());
	        XMLStreamReader streamReader = inputFactory.createXMLStreamReader(reader);
	                
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
		                    		payload.addMetric(name, (String)value);
		                    		break;
		                        }
		                    	case "double": {
		                    		payload.addMetric(name, Double.parseDouble(value));
		                            break;
		                        }
		                    	case "int": {
		                    		payload.addMetric(name, Integer.parseInt(value));
		                    		break;
		                        }
		                    	case "float": {
		                    		payload.addMetric(name, Float.parseFloat(value));
		                            break;
		                        }
		                    	case "long": {
		                    		payload.addMetric(name, Long.parseLong(value));
		                            break;
		                        }
		                    	case "boolean": {
		                    		payload.addMetric(name, Boolean.parseBoolean(value));
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
	        s_logger.info("KuraPayload Metrics: " + payload.metrics().toString());	 
	        latestPayloads.put(topic, payload);
		}
		catch(Exception e){
			s_logger.info(e.toString());
		}
		
	}
	
	private void doPublish() 
	{				
        //Publish for each topic in the HashMap & its metrics
		for (Map.Entry<String, KuraPayload> entry : latestPayloads.entrySet()) {
		    String topic = entry.getKey();
		    KuraPayload payload = entry.getValue();

		    try {
				m_cloudClient.publish(topic, payload, 0, false);
				s_logger.info("Published to {} message: {}", topic, payload);
			} 
			catch (Exception e) {
				s_logger.error("Cannot publish topic: "+ topic, e);
			}
		}
	}
}