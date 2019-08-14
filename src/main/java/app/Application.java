package app;

import org.apache.logging.log4j.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.util.Properties;

@SpringBootApplication
public class Application {
    private Logger log = AppLogging.getLogger(Application.class);

    public static void main(String[] args) throws IOException {
        /*Appender appender = new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out");
        org.apache.log4j.BasicConfigurator.configure(appender);
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);
        rootLogger.addAppender(appender);*/

        //Properties configProperties = new Properties();
        //configProperties.load(new FileInputStream("config.properties"));
        //boolean amRoot = Boolean.parseBoolean(configProperties.getProperty("root_server", "false"));

        SpringApplication app = new SpringApplication(Application.class);
        app.run(args);
    }


}
