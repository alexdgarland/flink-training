package org.apache.flink.training.exercises.hourlytips;

public class HourlyTipsBasedOnSolutionTest extends AbstractHourlyTipsTest {

	@Override
	public Testable javaExercise() {
		return () -> HourlyTipsExerciseBasedOnSolution.main(new String[]{});
	}

}
