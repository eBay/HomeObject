#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, HSHomeObjectCPTestBasic) {
    // Step-1: create a PG and a shard
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    create_pg(1 /* pg_id */);
    auto shard_info = create_shard(1 /* pg_id */, 64 * Mi);
    pg_shard_id_vec.emplace_back(1 /* pg_id */, shard_info.id);
    LOGINFO("pg {} shard {}", 1, shard_info.id);
    {
        // Step-2: write some dirty pg information and add to dirt list;
        auto lg = std::unique_lock(_obj_inst->_pg_lock);
        for (auto& [_, pg] : _obj_inst->_pg_map) {
            auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());
            hs_pg->durable_entities_.blob_sequence_num = 54321; // fake some random blob seq number to make it dirty;
            hs_pg->is_dirty_.store(true);

            // test multiple update to the dirty list;
            // only the last update should be kept;
            hs_pg->durable_entities_.blob_sequence_num = 12345; // fake some random blob seq number to make it dirty;
            hs_pg->is_dirty_.store(true);
        }
    }

    restart();

    EXPECT_TRUE(_obj_inst->_pg_map.size() == 1);
    {
        auto lg = std::shared_lock(_obj_inst->_pg_lock);
        for (auto& [_, pg] : _obj_inst->_pg_map) {
            auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());
            EXPECT_EQ(hs_pg->durable_entities_.blob_sequence_num, 12345);
        }
    }
}
