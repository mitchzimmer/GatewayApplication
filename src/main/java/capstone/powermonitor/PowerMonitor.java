package capstone.powermonitor;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudClientListener;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.message.KuraPayload;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.ComponentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PowerMonitor implements ConfigurableComponent, CloudClientListener  
{	
	private static final Logger s_logger = LoggerFactory.getLogger(PowerMonitor.class);
	
	// Cloud Application identifier
	private static final String APP_ID = "PeakMonitor";

	// Publishing Property Names
	private static final String   PUBLISH_RATE_PROP_NAME   = "publish.rate";
	
	private CloudService                m_cloudService;
	private CloudClient      			m_cloudClient;
	
	private ScheduledExecutorService    m_worker;
	private ScheduledFuture<?>          m_handle;
	
	private Map<String, Object>         m_properties;
	
	
	// ----------------------------------------------------------------
	//
	//   Dependencies
	//
	// ----------------------------------------------------------------
	
	public PowerMonitor() 
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
		
		m_properties = properties;
		for (String s : properties.keySet()) {
			s_logger.info("Activate - "+s+": "+properties.get(s));
		}
		
		// get the mqtt client for this application
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

		s_logger.debug("Deactivating " + APP_ID + "... Done.");
	}	
	
	
	public void updated(Map<String,Object> properties)
	{
		s_logger.info("Updated " + APP_ID + "...");

		// store the properties received
		m_properties = properties;
		for (String s : properties.keySet()) {
			s_logger.info("Update - "+s+": "+properties.get(s));
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
	public void onControlMessageArrived(String deviceId, String appTopic,
			KuraPayload msg, int qos, boolean retain) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onMessageArrived(String deviceId, String appTopic,
			KuraPayload msg, int qos, boolean retain) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onConnectionLost() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onConnectionEstablished() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onMessageConfirmed(int messageId, String appTopic) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onMessagePublished(int messageId, String appTopic) {
		// TODO Auto-generated method stub
		
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
			}
		}, 0, pubrate, TimeUnit.MINUTES); //FIXME: Remove multiplier of *30 to pubrate
	}
	
	
	/**
	 * Called at the configured rate to publish the next temperature measurement.
	 */
	private void doPublish() 
	{				
		// fetch the publishing configuration from the publishing properties
		String deviceMac = "C4-F9-D5-1D-45-A4";
		String  topic  = deviceMac;
		
		String deviceMac2 = "0A-2C-26-DF-A7-24";
		String  topic2  = deviceMac2;
		
		// Allocate a new payload
		KuraPayload payload = new KuraPayload();
		KuraPayload payload2 = new KuraPayload();
		
		// Timestamp the message
		payload.setTimestamp(new Date());
		payload2.setTimestamp(new Date());

		//Generate Random Current and Voltage vars to get our Power
		double max = 10.0;
		double min = 0.0;
		
		Double current = ThreadLocalRandom.current().nextDouble(min, max);;
		Double voltage = ThreadLocalRandom.current().nextDouble(min, max);;
		Double power = voltage * current;

		Double current2 = ThreadLocalRandom.current().nextDouble(min, max);;
		Double voltage2 = ThreadLocalRandom.current().nextDouble(min, max);;
		Double power2 = voltage * current;
		
		// Add the Voltage, Current, and Power as a metric to the payload
		payload.addMetric("Voltage", voltage);
		payload.addMetric("Current", current);
		payload.addMetric("Power",  power);
		
		payload2.addMetric("Voltage", voltage2);
		payload2.addMetric("Current", current2);
		payload2.addMetric("Power",  power2);
		
		// Publish the message
		try {
			m_cloudClient.publish(topic, payload, 0, false);
			s_logger.info("Published to {} message: {}", topic, payload);
			
			m_cloudClient.publish(topic2, payload2, 0, false);
			s_logger.info("Published to {} message: {}", topic2, payload2);
		} 
		catch (Exception e) {
			s_logger.error("Cannot publish topic: "+ topic + " AND/OR " + topic2, e);
		}
		
	}
}