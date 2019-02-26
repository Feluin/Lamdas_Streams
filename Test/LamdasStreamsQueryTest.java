import las.lamdas_and_streams.StreamQuery;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LamdasStreamsQueryTest {
    private StreamQuery query;

    @BeforeAll
    public void initTests() {
        query = new StreamQuery();
        query.startup("./Test/TestRecords.csv");
    }


    @Test
    public void test1() {
        Long res = 1000000L;
        assertEquals(res, query.execute01());
    }

    @Test
    public void test2() {
        Long res = 11129L;
        assertEquals(res, query.execute02());
    }

    @Test
    public void test3() {
        Long res = 2692L;
        assertEquals(res, query.execute03());
    }

    @Test
    public void test4() {
        Long res = 1385L;
        assertEquals(res, query.execute04());
    }

    @Test
    public void test5() {
        Double res = 417574.0;
        assertEquals(res, query.execute05());
    }

    @Test
    public void test6() {
        Double res = 2753.00407000407;
        assertEquals(res, query.execute06());
    }

    @Test
    public void test7() {
        List<Integer> res = List.of(329541, 1507);
        assertEquals(res, query.execute07());
    }

    @Test
    public void test8() {
        List<Integer> res = List.of(855266, 165461, 201896, 354929, 943502, 678440, 583418);
        assertEquals(res, query.execute08());
    }

    @Test
    public void test9() {
        Map<String,Long> res=new HashMap<>();
        res.put("true",79568L);
        res.put("false",920432L);
        assertEquals(res, query.execute09());
    }

    @Test
    public void test10() {
        Map<Character,Long> res=new HashMap<>();
        res.put('S',26157L);
        res.put('L',26468L);
        res.put('M',26217L);
        assertEquals(res, query.execute10());
    }

    @Test
    public void test11() {
        Map<Character,Long> res=new HashMap<>();
        res.put('A',98900L);
        res.put('C',98896L);
        assertEquals(res, query.execute11());
    }

    @Test
    public void test12() {
        Map<Character,Long> res=new HashMap<>();
        res.put('A',3L);
        res.put('B',2L);
        res.put('D',2L);
        res.put('E',2L);
        res.put('F',2L);
        res.put('H',1L);
        res.put('I',5L);
        res.put('J',2L);
        assertEquals(res, query.execute12());
    }

    @Test
    public void test13() {
        Map<String,Double> res=new HashMap<>();
        res.put("true",512366.0);
        res.put("false",5.6313199E7);
        assertEquals(res, query.execute13());
    }

    @Test
    public void test14() {
        Map<Character,Double> res=new HashMap<>();
        res.put('C',3074.322033898305);
        res.put('D',3155.7630662020906);
        assertEquals(res, query.execute14());
    }

}
