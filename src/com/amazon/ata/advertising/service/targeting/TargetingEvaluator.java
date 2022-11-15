package com.amazon.ata.advertising.service.targeting;

import com.amazon.ata.advertising.service.model.RequestContext;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicate;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicateResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Evaluates TargetingPredicates for a given RequestContext.
 */
public class TargetingEvaluator {
    public static final boolean IMPLEMENTED_STREAMS = true;
    public static final boolean IMPLEMENTED_CONCURRENCY = true;
    private final RequestContext requestContext;

    /**
     * Creates an evaluator for targeting predicates.
     * @param requestContext Context that can be used to evaluate the predicates.
     */
    public TargetingEvaluator(RequestContext requestContext) {
        this.requestContext = requestContext;
    }

    /**
     * Evaluate a TargetingGroup to determine if all of its TargetingPredicates are TRUE or not for the given
     * RequestContext.
     * @param targetingGroup Targeting group for an advertisement, including TargetingPredicates.
     * @return TRUE if all of the TargetingPredicates evaluate to TRUE against the RequestContext, FALSE otherwise.
     */
    public TargetingPredicateResult evaluate(TargetingGroup targetingGroup) {
        List<TargetingPredicate> targetingPredicates = targetingGroup.getTargetingPredicates();

        //initial code using a for loop:
//        boolean allTruePredicates = true;
//        for (TargetingPredicate predicate : targetingPredicates) {
//            TargetingPredicateResult predicateResult = predicate.evaluate(requestContext);
//            if (!predicateResult.isTrue()) {
//                allTruePredicates = false;
//                break;
//            }
//        }

        // MT1 Sprint 25 replacing for loop with Stream and Lambda functions:
//        boolean allTruePredicates = targetingPredicates.stream()
//                .map(targetingPredicate -> targetingPredicate.evaluate(requestContext))
//                .allMatch(targetingPredicateResult -> targetingPredicateResult.isTrue());

        //MT2 Sprint 26 use concurrent calls to evaluate each targetingPredicate:
        // concurrently call targetingPredicate.evaluate(requestContext)
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<TargetingPredicateResult>> futureList = new ArrayList<>();

        targetingPredicates.stream()
                .forEach(targetingPredicate -> {
                    targetingPredicate.setRequestContext(requestContext);
                    futureList.add(executorService.submit(targetingPredicate));
                });
        executorService.shutdown();

        boolean allTruePredicates = futureList.stream()
                .allMatch(future -> {
                    try {
                        return future.get().isTrue();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });

        return allTruePredicates ? TargetingPredicateResult.TRUE :
                                   TargetingPredicateResult.FALSE;
    }
}
