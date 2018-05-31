package lemongrenade.examples.test;

public class StormTest {
    public static void main(String[] args) throws Exception {
        LocalEngineTester test = new LocalEngineTester();
        String [] filename = {"test-topology.json"};
        test.main(filename);
    }
}