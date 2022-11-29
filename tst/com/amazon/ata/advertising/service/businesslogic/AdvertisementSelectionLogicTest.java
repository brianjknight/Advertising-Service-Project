package com.amazon.ata.advertising.service.businesslogic;

import com.amazon.ata.advertising.service.dao.ReadableDao;
import com.amazon.ata.advertising.service.model.AdvertisementContent;
import com.amazon.ata.advertising.service.model.EmptyGeneratedAdvertisement;
import com.amazon.ata.advertising.service.model.GeneratedAdvertisement;
import com.amazon.ata.advertising.service.targeting.TargetingEvaluator;
import com.amazon.ata.advertising.service.targeting.TargetingGroup;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicate;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicateResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class AdvertisementSelectionLogicTest {

    private static final String CUSTOMER_ID = "A123B456";
    private static final String MARKETPLACE_ID = "1";

    private static final String CONTENT_ID1 = UUID.randomUUID().toString();
    private static final AdvertisementContent CONTENT1 = AdvertisementContent.builder().withContentId(CONTENT_ID1).build();
    private static final String CONTENT_ID2 = UUID.randomUUID().toString();
    private static final AdvertisementContent CONTENT2 = AdvertisementContent.builder().withContentId(CONTENT_ID2).build();
    private static final String CONTENT_ID3 = UUID.randomUUID().toString();
    private static final AdvertisementContent CONTENT3 = AdvertisementContent.builder().withContentId(CONTENT_ID3).build();
    private static final String CONTENT_ID4 = UUID.randomUUID().toString();
    private static final AdvertisementContent CONTENT4 = AdvertisementContent.builder().withContentId(CONTENT_ID4).build();

    @Mock
    private ReadableDao<String, List<AdvertisementContent>> contentDao;

    @Mock
    private ReadableDao<String, List<TargetingGroup>> targetingGroupDao;

    @Mock
    private Random random;

    @Mock
    private TargetingEvaluator evaluator;

    private AdvertisementSelectionLogic adSelectionService;

    @BeforeEach
    public void setup() {
        initMocks(this);
        adSelectionService = new AdvertisementSelectionLogic(contentDao, targetingGroupDao);
        adSelectionService.setRandom(random);
    }

    @Test
    public void selectAdvertisement_nullMarketplaceId_EmptyAdReturned() {
        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, null);
        assertTrue(ad instanceof EmptyGeneratedAdvertisement);
    }

    @Test
    public void selectAdvertisement_emptyMarketplaceId_EmptyAdReturned() {
        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, "");
        assertTrue(ad instanceof EmptyGeneratedAdvertisement);
    }

    @Test
    public void selectAdvertisement_noContentForMarketplace_emptyAdReturned() throws InterruptedException {
        when(contentDao.get(MARKETPLACE_ID)).thenReturn(Collections.emptyList());

        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, MARKETPLACE_ID);

        assertTrue(ad instanceof EmptyGeneratedAdvertisement);
    }

    @Test
    public void selectAdvertisement_oneAd_returnsAd() {
        //GIVEN
        List<AdvertisementContent> contents = Arrays.asList(CONTENT1);
        List<TargetingGroup> targetingGroupList = new ArrayList<>();
        TargetingGroup targetingGroup = new TargetingGroup();
        targetingGroup.setClickThroughRate(.5);
        targetingGroup.setContentId(CONTENT_ID1);
        targetingGroup.setTargetingPredicates(new ArrayList<>());
        targetingGroupList.add(targetingGroup);

        when(contentDao.get(MARKETPLACE_ID)).thenReturn(contents);
        when(targetingGroupDao.get(CONTENT_ID1)).thenReturn(targetingGroupList);
        when(evaluator.evaluate(any(TargetingGroup.class))).thenReturn(TargetingPredicateResult.TRUE);

        //WHEN
        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, MARKETPLACE_ID);

        //THEN
        assertEquals(CONTENT_ID1, ad.getContent().getContentId());
    }

    // TODO update test checking that it returns the Ad with highest click through rate instead of a random ad
    //  selectAdvertisement_multipleAds_returnsHighestClickThroughRate()
    @Test
    public void selectAdvertisement_multipleAds_returnsOneRandom() {
       //GIVEN
        List<AdvertisementContent> contents = Arrays.asList(CONTENT1, CONTENT2, CONTENT3);
       // create 3 List<TargetingGroup> objects with different click-through rates.
        List<TargetingGroup> targetingGroupList1 = new ArrayList<>();
        TargetingGroup targetingGroup1 = new TargetingGroup();
        targetingGroup1.setClickThroughRate(.1);
        targetingGroup1.setContentId(CONTENT_ID1);
        targetingGroup1.setTargetingPredicates(new ArrayList<>());
        targetingGroupList1.add(targetingGroup1);

        List<TargetingGroup> targetingGroupList2 = new ArrayList<>();
        TargetingGroup targetingGroup2 = new TargetingGroup();
        targetingGroup2.setClickThroughRate(.5);
        targetingGroup2.setContentId(CONTENT_ID2);
        targetingGroup2.setTargetingPredicates(new ArrayList<>());
        targetingGroupList2.add(targetingGroup2);

        List<TargetingGroup> targetingGroupList3 = new ArrayList<>();
        TargetingGroup targetingGroup3 = new TargetingGroup();
        targetingGroup3.setClickThroughRate(.2);
        targetingGroup3.setContentId(CONTENT_ID3);
        targetingGroup3.setTargetingPredicates(new ArrayList<>());
        targetingGroupList3.add(targetingGroup3);

        when(contentDao.get(MARKETPLACE_ID)).thenReturn(contents);
        when(targetingGroupDao.get(CONTENT_ID1)).thenReturn(targetingGroupList1);
        when(targetingGroupDao.get(CONTENT_ID2)).thenReturn(targetingGroupList2);
        when(targetingGroupDao.get(CONTENT_ID3)).thenReturn(targetingGroupList3);

        when(evaluator.evaluate(any(TargetingGroup.class))).thenReturn(TargetingPredicateResult.TRUE);

       //WHEN
        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, MARKETPLACE_ID);

        //THEN
        assertEquals(CONTENT_ID2, ad.getContent().getContentId());
    }

}
