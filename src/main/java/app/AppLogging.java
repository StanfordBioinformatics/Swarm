package app;

import org.apache.logging.log4j.*;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;

public class AppLogging {
    private static Level appLogLevel = Level.DEBUG;

    static {
        //Appender appender = new ConsoleAppender(
        //        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out");
        //org.apache.log4j.BasicConfigurator.configure(appender);
        Logger rootLogger = LogManager.getRootLogger();
        //rootLogger.setLevel(Level.DEBUG);
        Configurator.setLevel(rootLogger.getName(), Level.INFO);
        //rootLogger.setLevel(Level.INFO);
        //rootLogger.addAppender(appender);
    }

    public static Logger getLogger(Class c) {
        Logger l = LogManager.getLogger(c);
        Configurator.setLevel(l.getName(), appLogLevel);
        return l;
    }
}
