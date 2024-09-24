
package msftrakesh.redis.lettuce.retry;  
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class AppSettings {
       
        private String redisHostName;
        private int redisPort;
        private String redisKey;
        private int reconnectMinInterval;
        private int reconnectMaxDuration;
        private String logLevel;


        public AppSettings(String configFilePath) {
            loadConfig(configFilePath);
        }

        private void loadConfig(String configFilePath) {
            Yaml yaml = new Yaml();
            try (InputStream inputStream = new FileInputStream(configFilePath)) {
                if (inputStream == null) {
                    System.out.println("File not found!");
                } else {
                    Map<String, Object> config = yaml.load(inputStream);
                    this.redisHostName = (String) ((Map<String, Object>) ((Map<String, Object>) config.get("app")).get("settings")).get("redisHostName");
                    this.redisKey = (String) ((Map<String, Object>) ((Map<String, Object>) config.get("app")).get("settings")).get("redisKey");
                    this.redisPort = (int) ((Map<String, Object>) ((Map<String, Object>) config.get("app")).get("settings")).get("redisPort");

                    this.reconnectMaxDuration = (int) ((Map<String, Object>) ((Map<String, Object>) config.get("app")).get("settings")).get("reconnectMaxDuration");
                    this.reconnectMinInterval = (int) ((Map<String, Object>) ((Map<String, Object>) config.get("app")).get("settings")).get("reconnectMinInterval");

                    this.logLevel = (String) ((Map<String, Object>) ((Map<String, Object>) config.get("app")).get("settings")).get("logLevel");
                     
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public String getRedisHostName() {
            return redisHostName;
        }

        public String getRedisKey() {
            return redisKey;
        }

        public int getReconnectMinInterval() {
            return reconnectMinInterval;
        }

        public int getRedisPort() {
            return redisPort;
        }

        public int getReconnectMaxDuration() {
            return reconnectMaxDuration;
        }

        public String getLogLevel() {
            return logLevel;
        }



    }


