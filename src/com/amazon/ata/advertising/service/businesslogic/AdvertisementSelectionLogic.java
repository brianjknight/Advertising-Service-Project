package com.amazon.ata.advertising.service.businesslogic;

import com.amazon.ata.advertising.service.dao.ReadableDao;
import com.amazon.ata.advertising.service.model.AdvertisementContent;
import com.amazon.ata.advertising.service.model.EmptyGeneratedAdvertisement;
import com.amazon.ata.advertising.service.model.GeneratedAdvertisement;
import com.amazon.ata.advertising.service.model.RequestContext;
import com.amazon.ata.advertising.service.targeting.TargetingEvaluator;
import com.amazon.ata.advertising.service.targeting.TargetingGroup;

import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicateResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;

/**
 * This class is responsible for picking the advertisement to be rendered.
 */
public class AdvertisementSelectionLogic {

    private static final Logger LOG = LogManager.getLogger(AdvertisementSelectionLogic.class);

    private final ReadableDao<String, List<AdvertisementContent>> contentDao;
    private final ReadableDao<String, List<TargetingGroup>> targetingGroupDao;
    private Random random = new Random();

    /**
     * Constructor for AdvertisementSelectionLogic.
     * @param contentDao Source of advertising content.
     * @param targetingGroupDao Source of targeting groups for each advertising content.
     */
    @Inject
    public AdvertisementSelectionLogic(ReadableDao<String, List<AdvertisementContent>> contentDao,
                                       ReadableDao<String, List<TargetingGroup>> targetingGroupDao) {
        this.contentDao = contentDao;
        this.targetingGroupDao = targetingGroupDao;
    }

    /**
     * Setter for Random class.
     * @param random generates random number used to select advertisements.
     */
    public void setRandom(Random random) {
        this.random = random;
    }

    /**
     * Gets all of the content and metadata for the marketplace and determines which content can be shown.  Returns the
     * eligible content with the highest click through rate.  If no advertisement is available or eligible, returns an
     * EmptyGeneratedAdvertisement.
     *
     * @param customerId - the customer to generate a custom advertisement for
     * @param marketplaceId - the id of the marketplace the advertisement will be rendered on
     * @return an advertisement customized for the customer id provided, or an empty advertisement if one could
     *     not be generated.
     */
    public GeneratedAdvertisement selectAdvertisement(String customerId, String marketplaceId) {
        GeneratedAdvertisement generatedAdvertisement = new EmptyGeneratedAdvertisement();
        if (StringUtils.isEmpty(marketplaceId)) {
            LOG.warn("MarketplaceId cannot be null or empty. Returning empty ad.");
        } else {
            final List<AdvertisementContent> contents = contentDao.get(marketplaceId);

            // TODO update class for dependency injection
            RequestContext requestContext = new RequestContext(customerId, marketplaceId);
            TargetingEvaluator evaluator = new TargetingEvaluator(requestContext);

            // TODO convert loop below to a stream (and an optional?) to return the filtered content list
            List<AdvertisementContent> filteredContent = new ArrayList<>();
            //we want to filter List<AdvertisementContent> contents for only eligible ads for the customerId
            for (AdvertisementContent content : contents) {
                // each AdvertisementContent has its contentId
                // contentId is used to retrieve List<TargetingGroup> from TargetingGroupDao
                List<TargetingGroup> tempTargetingGroupList = targetingGroupDao.get(content.getContentId());
                for (TargetingGroup targetingGroup : tempTargetingGroupList) {
                    // the evaluator was instantiated above using the input customerId & marketplaceID to evaluate against
                    // if any one of the target groups evaluates to isTrue, the content is eligible
                    if (evaluator.evaluate(targetingGroup).isTrue()) {
                        filteredContent.add(content);
                        continue;
                    }
                }
            }

            List<AdvertisementContent> filteredContentFromStream = contents.stream()
                    .filter( advertisementContent -> {
                        return targetingGroupDao.get(advertisementContent.getContentId()).stream()
                                .map(evaluator::evaluate)
                                .anyMatch(TargetingPredicateResult::isTrue);
                    })
                    .collect(Collectors.toList());

            System.out.println("filteredContent(using loops) list result : \n" + filteredContent);
            System.out.println("filteredContentFromStream list result : \n" + filteredContentFromStream);

            if (CollectionUtils.isNotEmpty(filteredContentFromStream)) {
                AdvertisementContent randomAdvertisementContent = filteredContentFromStream.get(random.nextInt(filteredContentFromStream.size()));
                generatedAdvertisement = new GeneratedAdvertisement(randomAdvertisementContent);
            }

        }

        return generatedAdvertisement;
    }
}
