#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, PGStatsTest) {
    LOGINFO("HomeObject replica={} setup completed", g_helper->replica_num());
    //  Create a pg, shard, put blob should succeed.
    pg_id_t pg_id{1};
    create_pg(pg_id);
    auto shard_info = create_shard(pg_id, 64 * Mi);
    auto shard_id = shard_info.id;
    auto s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);
    LOGINFO("Got shard {}", shard_info.id);
    shard_info = s.value();

    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::OPEN);
    put_blob(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul});

    // seal the shard
    shard_info = seal_shard(shard_id);
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);
    LOGINFO("Sealed shard {}", shard_id);

    // create a 2nd shard
    auto shard_info2 = create_shard(pg_id, 64 * Mi);
    auto shard_id2 = shard_info2.id;
    auto s2 = _obj_inst->shard_manager()->get_shard(shard_id2).get();
    ASSERT_TRUE(!!s2);
    LOGINFO("Got shard {}", shard_id2);

    PGStats pg_stats;
    auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
    LOGINFO("stats: {}", pg_stats.to_string());

    EXPECT_EQ(res, true);
    EXPECT_EQ(pg_stats.id, pg_id);
    EXPECT_EQ(pg_stats.total_shards, 2);
    EXPECT_EQ(pg_stats.open_shards, 1);
    // we have 3-replica test frame work now
    EXPECT_EQ(pg_stats.num_members, g_helper->members().size());

    auto stats = _obj_inst->get_stats();
    LOGINFO("HomeObj stats: {}", stats.to_string());
}

TEST_F(HomeObjectFixture, PGRecoveryTest) {
    // create 10 pg
    for (pg_id_t i = 1; i < 11; i++) {
        pg_id_t pg_id{i};
        create_pg(pg_id);
    }

    // get pg map
    HSHomeObject* ho = dynamic_cast< HSHomeObject* >(_obj_inst.get());
    std::map< pg_id_t, std::unique_ptr< PG > > pg_map;
    pg_map.swap(ho->_pg_map);

    // get uuid
    auto id = ho->our_uuid();

    // restart
    restart();

    // verify uuid
    EXPECT_EQ(id, _obj_inst->our_uuid());

    // verify pg map
    EXPECT_EQ(10, _obj_inst->_pg_map.size());

    for (auto const& [id, pg] : _obj_inst->_pg_map) {
        EXPECT_TRUE(pg_map.contains(id));
        auto reserved_pg = dynamic_cast< HSHomeObject::HS_PG* >(pg_map[id].get());
        auto recovered_pg = dynamic_cast< HSHomeObject::HS_PG* >(pg.get());
        EXPECT_TRUE(reserved_pg);
        EXPECT_TRUE(recovered_pg);
        verify_hs_pg(reserved_pg, recovered_pg);
    }
}
