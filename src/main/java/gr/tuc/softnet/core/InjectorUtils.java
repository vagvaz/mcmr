package gr.tuc.softnet.core;

import com.google.inject.Injector;

/**
 * Created by vagvaz on 21/03/16.
 */
public class InjectorUtils {
    static Injector injector;

    public  static void setInjector(Injector injector){
        InjectorUtils.injector = injector;
    }

    public static Injector getInjector(){
        return injector;
    }
}
