package examplestests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by ap0n on 28/4/2016.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestKmeansDocument.class,
    TestCountMinDocument.class,
    TestSketch.class
})
public class ExamplesTestSuite { }
