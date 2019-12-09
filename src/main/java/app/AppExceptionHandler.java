package app;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

@RestControllerAdvice
public class AppExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Object> handleException(Exception e, HttpServletRequest request)
            throws UnsupportedEncodingException {
        //log.warn(e.getStackTrace());
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        PrintWriter pw = new PrintWriter(os);
        e.printStackTrace(pw);

        e.printStackTrace();
        String stacktrace = os.toString(StandardCharsets.UTF_8.name());
        return new ResponseEntity<>(stacktrace, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
