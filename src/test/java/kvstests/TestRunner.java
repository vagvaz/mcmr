package kvstests;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Created by ap0n on 5/4/2016.
 */
public class TestRunner {

  public static void main(String[] args) {
    Result result = JUnitCore.runClasses(PipelineSingleTest.class);
    for (Failure f : result.getFailures()) {
      System.out.println(f.toString());
    }
    if (result.wasSuccessful()) {
      System.out.println("Test completed successfully");
    } else {
      System.out.println("Test failed");
    }
  }
}
