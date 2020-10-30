package com.maths22.infima;

import com.amazonaws.services.route53.infima.*;

import java.util.*;

public class Main {
    public static void main(String[] args) throws StatefulSearchingShuffleSharder.NoShardsAvailableException {

//        System.out.println("DB Clusters, App Clusters");
        int[][] options = {
                {2, 1},
                {3, 1},
                {4, 1},
                {4, 2},
//                {5, 1},
                {5, 2},
//                {6, 1},
//                {6, 2},
                {3, 2},
                {6, 3},
                {6, 4}
        };
        System.out.println("asgs_per_pgbouncer\tmaximum_overlap\trisk\tbreakeven_asgs\tiad_asgs\tpdx_asgs");
        for(int[] option : options){

//            System.out.println(option[0] + " pools each, " + option[1] + " overlap");
            int uniqEndpoints = option[0];
            boolean foundBreakEven = false;
            int breakEven = -1;
            int pdx = -1;
            int iad = -1;
            for (int clusterCount = 5; clusterCount < 196; clusterCount++) {
                Set<Set<String>> shards = assignShards(uniqEndpoints, clusterCount, option[0], option[1]);
                uniqEndpoints = (int) shards.stream().flatMap(Collection::stream).distinct().count();
                if (clusterCount >= uniqEndpoints && !foundBreakEven) {
                    foundBreakEven = true;
                    breakEven = Math.max(uniqEndpoints, clusterCount);
                }
                if (clusterCount == 195) {
                    iad = uniqEndpoints;
                }
                if (clusterCount == 60) {
                    pdx = uniqEndpoints;
                }
            }
            System.out.println(option[0] + "\t" + option[1] + "\t" + (int)(option[1] / (float)option[0] * 100) + "%\t" + breakEven + "\t" + iad + "\t" + pdx);
        }
    }

    private static Set<Set<String>> assignShards(int endpointCount, int clusterCount, int endpointsPerCell, int maximumOverlap) {
        for (; endpointCount < 100; endpointCount += 1) {
            Set<Set<String>> shardSets = new HashSet<>();
            try {
                SingleCellLattice<String> lattice = new SingleCellLattice<>();
                for (int i = 0; i < endpointCount; i++) {
                    lattice.addEndpoint("poolS" + i);
                }
                StatefulSearchingShuffleSharder<String> sharder = new StatefulSearchingShuffleSharder<>(new MockFragmentStore());
                for (int i = 0; i < clusterCount; i++) {
                    shardSets.add(new HashSet<>(sharder.shuffleShard(lattice, endpointsPerCell, maximumOverlap).getAllEndpoints()));
                }
            } catch (StatefulSearchingShuffleSharder.NoShardsAvailableException ex) {
                continue;
            }
            return shardSets;
        }
        return null;
    }

    private static class MockFragmentStore implements StatefulSearchingShuffleSharder.FragmentStore<String> {
        private final HashSet<String> store = new HashSet<String>();

        @Override
        public void saveFragment(List<String> fragment) {
            Collections.sort(fragment);
            store.add(fragment.toString());
        }

        @Override
        public boolean isFragmentUsed(List<String> fragment) {
            Collections.sort(fragment);
            return store.contains(fragment.toString());
        }
    }

}
