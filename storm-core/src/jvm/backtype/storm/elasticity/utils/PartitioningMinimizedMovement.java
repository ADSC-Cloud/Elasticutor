package backtype.storm.elasticity.utils;

import java.util.*;

/**
 * Created by robert on 4/7/16.
 */
public class PartitioningMinimizedMovement {

    private List<Bin> bins = new ArrayList<Bin>();

    private int nbins;

    private Map<Integer, Integer> ballToBinMapping = new HashMap<Integer, Integer>();

    private Map<Integer, Long> ballToWeight;

    public PartitioningMinimizedMovement(int numberOfBins, Map<Integer, Integer> ballToBinMapping, Map<Integer, Long> ballToWeight) {
        this.ballToBinMapping.putAll(ballToBinMapping);
        this.ballToWeight = ballToWeight;
        nbins = numberOfBins;
        for(int i = 0; i < numberOfBins; i++) {
            bins.add(new Bin(Long.MAX_VALUE, i));
        }
        for(int ball: ballToBinMapping.keySet()) {
            int bin = ballToBinMapping.get(ball);
            long weight = ballToWeight.get(ball);
            bins.get(bin).put(weight);
        }


    }

    /** Try to move balls among bins to balance the weight of bins, while minimizing the movements.
     * This problem is NP-hard, so we conduct heuristic solution.**/
    public Map<Integer, Integer> getSolution() {
        Map<Integer, Set<Ball>> binToBalls = new HashMap<>();
        for(int i = 0; i < nbins; i++) {
            binToBalls.put(i, new HashSet<Ball>());
        }
        for(int ball: ballToBinMapping.keySet()) {
            int bin = ballToBinMapping.get(ball);
            long weight = ballToWeight.get(ball);
            binToBalls.get(bin).add(new Ball(ball,weight));
        }

        boolean validMovement = true;
        final Bin.BinComparator binComparator = new Bin.BinComparator();
        while(validMovement) {
            validMovement = false;
            Collections.sort(bins, binComparator);

            Bin smallestBin = bins.get(0);
            Bin largestBin = bins.get(nbins - 1);
            Set<Ball> ballsInTheLargestBin = binToBalls.get(largestBin.index);
            List<Ball> ballsInTheLargestBinList = new ArrayList<Ball>();
            ballsInTheLargestBinList.addAll(ballsInTheLargestBin);
            Collections.sort(ballsInTheLargestBinList, new Ball.BallReverseComparator());
            for(int i = 0; i < ballsInTheLargestBinList.size(); i++) {
//            for(Ball ball: ballsInTheLargestBin) {
                Ball ball = ballsInTheLargestBinList.get(i);
                if(ball.size == 0) {
                    // avoid moving empty balls!
//                    break;
                }
                if(largestBin.currentSize - smallestBin.currentSize > ball.size) {
                    // a valid movement is found!
                    validMovement = true;
                    smallestBin.put(ball.size);
                    largestBin.remove(ball.size);
                    ballToBinMapping.put(ball.index, smallestBin.index);
                    binToBalls.get(largestBin.index).remove(ball);
                    binToBalls.get(smallestBin.index).add(ball);
                    break;
                }
            }


        }

        return ballToBinMapping;
    }


    public static void main(String[] args) {
        System.out.println("Hello world!");

        Map<Integer, Integer> ballToBinMappting = new HashMap<>();
        ballToBinMappting.put(0,0);
        ballToBinMappting.put(1,3);
        ballToBinMappting.put(2,3);
        ballToBinMappting.put(3,3);
        ballToBinMappting.put(4,2);
        ballToBinMappting.put(5,2);
        ballToBinMappting.put(6,2);




        Map<Integer, Long> ballToWeight = new HashMap<>();
        ballToWeight.put(0,100L);
        ballToWeight.put(1,400L);
        ballToWeight.put(2,300L);
        ballToWeight.put(3,300L);
        ballToWeight.put(4,600L);
        ballToWeight.put(5,200L);
        ballToWeight.put(6,100L);


        final int numberOfBins = 4;

        List<Bin> oldBin = new ArrayList<>();
        for(int i = 0; i < numberOfBins; i++ ) {
            oldBin.add(new Bin(Long.MAX_VALUE, i));
        }


        for(int ball: ballToBinMappting.keySet()) {
            oldBin.get(ballToBinMappting.get(ball)).put(ballToWeight.get(ball));
        }

        PartitioningMinimizedMovement solver = new PartitioningMinimizedMovement(4, ballToBinMappting, ballToWeight);
        Map<Integer, Integer> optimizedBallToBinMapping = solver.getSolution();

        for(Integer ball: optimizedBallToBinMapping.keySet()) {
            if(optimizedBallToBinMapping.get(ball)!=(ballToBinMappting.get(ball))) {
                System.out.println("Ball " + ball + " is moved from " + ballToBinMappting.get(ball) + " to " + optimizedBallToBinMapping.get(ball));
            }
        }


        List<Bin> newBins = new ArrayList<>();
        for(int i=0; i< numberOfBins; i++ ) {
            newBins.add(new Bin(Long.MAX_VALUE, i));
        }

        for(Integer ball: optimizedBallToBinMapping.keySet()) {
            newBins.get(optimizedBallToBinMapping.get(ball)).put(ballToWeight.get(ball));
        }

        System.out.println("Before: ");
        for(Bin bin: oldBin) {
            System.out.println("Bin" + bin.index + ": " + bin.currentSize);
        }


        System.out.println("After:");
        for(Bin bin: newBins) {
            System.out.println("Bin" + bin.index + ": " + bin.currentSize);
        }

        System.out.println("!");
    }
}
