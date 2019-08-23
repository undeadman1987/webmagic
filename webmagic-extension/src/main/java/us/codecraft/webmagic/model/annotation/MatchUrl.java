package us.codecraft.webmagic.model.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface MatchUrl {

    /**
     * The url patterns for class.<br>
     * Use regex expression with some changes: <br>
     *      "." stand for literal character "." instead of "any character". <br>
     *      "*" stand for any legal character for url in 0-n length ([^"'#]*) instead of "any length". <br>
     *
     * @return the url patterns for class
     */
    String[] value();
}
