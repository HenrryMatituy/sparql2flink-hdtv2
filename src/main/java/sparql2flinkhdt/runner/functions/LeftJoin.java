package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

//SolutionMapping - Flat Join Function
public class LeftJoin implements FlatJoinFunction<SolutionMappingHDT, SolutionMappingHDT, SolutionMappingHDT> {

    @Override
    public void join(SolutionMappingHDT left, SolutionMappingHDT right, Collector<SolutionMappingHDT> out) throws Exception {
        SolutionMappingHDT result = left.leftJoin(right);
        out.collect(result);
    }

}