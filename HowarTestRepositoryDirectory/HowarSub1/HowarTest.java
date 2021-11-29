import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class HowarTest {

    static class ScoreLevel{
        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }

        public int getMinScore() {
            return minScore;
        }

        public void setMinScore(int minScore) {
            this.minScore = minScore;
        }

        public int getMaxScore() {
            return maxScore;
        }

        public void setMaxScore(int maxScore) {
            maxScore = maxScore;
        }

        private String level;
        private int minScore;
        private int maxScore;

        public ScoreLevel(String level, int minScore, int maxScore){
            this.level = level;
            this.minScore = minScore;
            this.maxScore = maxScore;
        }
    }

    public static void main(String[] args){
        final List<ScoreLevel> scoreLevels = List.of(
                new ScoreLevel("Perfect",90,100),
                new ScoreLevel("Great", 80, 90),
                new ScoreLevel("Good", 70, 80),
                new ScoreLevel("Passed", 60, 70)
        );

        System.out.println("Please enter your score: ");
        int score = new Scanner(System.in).nextInt();
        if(score >= 0 && score <= 100){
            if(score < 60){
                System.out.println("Not passed!");
            }else {
                for(ScoreLevel scoreLevel : scoreLevels){
                    if(score>=scoreLevel.getMinScore() && score <= scoreLevel.getMaxScore()){
                        System.out.println("Your score level is: " + scoreLevel.getLevel());
                        break;
                    }
                }
            }
        }else{
            System.out.println("Invalid score value!");
        }
    }
}
