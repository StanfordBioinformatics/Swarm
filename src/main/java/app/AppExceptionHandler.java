package app;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;
import java.io.PrintWriter;
import java.io.StringWriter;

@RestControllerAdvice
public class AppExceptionHandler {

    @ExceptionHandler({Exception.class})
    public ResponseEntity<Object> handleException(Exception e, HttpServletRequest request) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        e.printStackTrace();
        String stacktrace = sw.toString();
        return new ResponseEntity<>(stacktrace, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
