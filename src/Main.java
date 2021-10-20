import common.Configuration;

public class Main {


    public static void main(String[] args) throws Exception {
        Configuration config = Configuration.initialize();
        Tester tester = new Tester(config);

        tester.testMVSubstitution();
//        tester.testBatch();
    }

}
