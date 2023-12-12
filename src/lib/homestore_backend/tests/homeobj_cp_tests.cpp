#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, HSHomeObjectCPTestBasic) {
    // Step-1: create a PG and a shard
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    create_pg(1 /* pg_id */);
    auto shard = _obj_inst->shard_manager()->create_shard(1 /* pg_id */, 64 * Mi).get();
    ASSERT_TRUE(!!shard);
    pg_shard_id_vec.emplace_back(1 /* pg_id */, shard->id);
    LOGINFO("pg {} shard {}", 1, shard->id);

    using namespace homestore;
    auto ho = dynamic_cast< HSHomeObject* >(_obj_inst.get());
    {
        // Step-2: write some dirty pg information and add to dirt list;
        auto cur_cp = HomeStore::instance()->cp_mgr().cp_guard();
        auto cp_ctx = s_cast< HomeObjCPContext* >(cur_cp->context(homestore::cp_consumer_t::HS_CLIENT));
        auto lg = std::unique_lock(ho->_pg_lock);
        for (auto& [_, pg] : ho->_pg_map) {
            auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());
            hs_pg->blob_sequence_num_ = 54321; // fake some random blob seq number to make it dirty;
            hs_pg->pg_sb_->blob_sequence_num = hs_pg->blob_sequence_num_;
            cp_ctx->add_pg_to_dirty_list(hs_pg->pg_sb_.get());
            hs_pg->blob_sequence_num_ = 54321; // fake some random blob seq number to make it dirty;

            // test multiple update to the dirty list;
            // only the last update should be kept;
            hs_pg->blob_sequence_num_ = 12345; // fake some random blob seq number to make it dirty;
            hs_pg->pg_sb_->blob_sequence_num = hs_pg->blob_sequence_num_;
            cp_ctx->add_pg_to_dirty_list(hs_pg->pg_sb_.get());
            hs_pg->blob_sequence_num_ = 12345; // fake some random blob seq number to make it dirty;
        }
    }

    // Step-3: trigger a cp;
    trigger_cp(true /* wait */);

    _obj_inst.reset();

    // Step-4: re-create the homeobject and pg infos and shard infos will be recover automatically.
    _obj_inst = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    ho = dynamic_cast< homeobject::HSHomeObject* >(_obj_inst.get());

    EXPECT_TRUE(ho->_pg_map.size() == 1);
    {
        auto lg = std::shared_lock(ho->_pg_lock);
        for (auto& [_, pg] : ho->_pg_map) {
            auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());
            EXPECT_EQ(hs_pg->pg_sb_->blob_sequence_num, 12345);
        }
    }
}
