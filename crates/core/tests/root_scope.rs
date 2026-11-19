use notion_knowledge_core::{
    backend::PageId,
    discovery::ExclusionRules,
    lifecycle::LifecycleScope,
    root_scope::{ScopeError, validate_scope},
};

const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";

fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 3,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    }
}

#[test]
fn policy_rejects_unconfigured_ambiguous_or_noncanonical_roots() {
    let mut cases = Vec::new();
    let mut empty = scope();
    empty.roots.clear();
    cases.push(empty);
    let mut duplicate = scope();
    duplicate.roots.push(PageId(ROOT.into()));
    cases.push(duplicate);
    let mut invalid = scope();
    invalid.roots = vec![PageId("https://www.notion.so/other".into())];
    cases.push(invalid);
    let mut upper = scope();
    upper.workspace_id = "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA".into();
    cases.push(upper);
    for policy in cases {
        assert!(matches!(
            validate_scope(&policy),
            Err(ScopeError::InvalidConfiguration)
        ));
    }
}

mod read_gate {
    use notion_knowledge_core::{
        backend::PageId,
        discovery::{ExclusionRules, SourceType},
        inventory::{InventoryEntry, InventoryParent},
        lifecycle::LifecycleScope,
        root_scope::{ReadScopeGate, ScopeError},
    };
    use std::{collections::BTreeMap, sync::Arc};

    const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    const MIDDLE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    const LEAF: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
    const ROW: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
    const OTHER_ROOT: &str = "ffffffff-ffff-ffff-ffff-ffffffffffff";

    fn entry(id: &str, parent: InventoryParent) -> (String, InventoryEntry) {
        (
            id.into(),
            InventoryEntry {
                page_id: id.into(),
                parent,
                root_id: ROOT.into(),
                last_edited_time: "2026-11-16T00:00:00Z".into(),
                in_trash: false,
            },
        )
    }

    /// ROOT > MIDDLE > LEAF, and ROW in a data source owned by MIDDLE.
    fn inventory() -> BTreeMap<String, InventoryEntry> {
        BTreeMap::from([
            entry(ROOT, InventoryParent::Root),
            entry(MIDDLE, InventoryParent::Page(ROOT.into())),
            entry(LEAF, InventoryParent::Page(MIDDLE.into())),
            entry(
                ROW,
                InventoryParent::DataSource {
                    data_source_id: "11111111-1111-1111-1111-111111111111".into(),
                    database_id: "22222222-2222-2222-2222-222222222222".into(),
                    owner_page_id: MIDDLE.into(),
                },
            ),
        ])
    }

    fn gate(
        inventory: BTreeMap<String, InventoryEntry>,
        exclusions: ExclusionRules,
    ) -> ReadScopeGate {
        ReadScopeGate::new(
            Arc::new(inventory),
            LifecycleScope {
                workspace_id: WORKSPACE.into(),
                generation: 1,
                roots: vec![PageId(ROOT.into())],
                exclusions,
            },
        )
        .unwrap()
    }

    #[test]
    fn gate_allows_page_with_chain_to_root_without_network() {
        // The gate holds only the inventory: it has no Notion port to call.
        let gate = gate(inventory(), ExclusionRules::default());
        for page in [ROOT, MIDDLE, LEAF, ROW] {
            let permit = gate.authorize(&PageId(page.into())).unwrap();
            assert!(permit.belongs_to_any(&[ROOT.into()]));
            assert!(!permit.belongs_to_any(&[OTHER_ROOT.into()]));
        }
    }

    #[test]
    fn gate_denies_page_absent_from_inventory() {
        let gate = gate(inventory(), ExclusionRules::default());
        assert_eq!(
            gate.authorize(&PageId(OTHER_ROOT.into())),
            Err(ScopeError::Denied)
        );
        assert_eq!(
            gate.authorize(&PageId("not a page".into())),
            Err(ScopeError::Denied)
        );
        // A chain broken by a missing ancestor vouches for nothing.
        let mut broken = inventory();
        broken.remove(MIDDLE);
        let gate = self::gate(broken, ExclusionRules::default());
        assert_eq!(
            gate.authorize(&PageId(LEAF.into())),
            Err(ScopeError::Denied)
        );
    }

    #[test]
    fn gate_denies_page_under_excluded_descendant() {
        let excluded = |rules: ExclusionRules| gate(inventory(), rules);
        let below_middle = excluded(ExclusionRules {
            descendants_of: [MIDDLE.to_owned()].into(),
            ..Default::default()
        });
        assert!(below_middle.authorize(&PageId(MIDDLE.into())).is_ok());
        for page in [LEAF, ROW] {
            assert_eq!(
                below_middle.authorize(&PageId(page.into())),
                Err(ScopeError::Denied)
            );
        }
        let middle = excluded(ExclusionRules {
            page_ids: [MIDDLE.to_owned()].into(),
            ..Default::default()
        });
        assert_eq!(
            middle.authorize(&PageId(LEAF.into())),
            Err(ScopeError::Denied)
        );
        let rows = excluded(ExclusionRules {
            source_types: [SourceType::DataSource].into(),
            ..Default::default()
        });
        assert_eq!(rows.authorize(&PageId(ROW.into())), Err(ScopeError::Denied));
        assert!(rows.authorize(&PageId(LEAF.into())).is_ok());
    }

    #[test]
    fn gate_denies_trashed_or_unreadable_links_and_unconfigured_roots() {
        let mut trashed = inventory();
        trashed.get_mut(MIDDLE).unwrap().in_trash = true;
        assert_eq!(
            gate(trashed, ExclusionRules::default()).authorize(&PageId(LEAF.into())),
            Err(ScopeError::Denied)
        );
        let mut unreadable = inventory();
        unreadable.get_mut(MIDDLE).unwrap().last_edited_time.clear();
        assert_eq!(
            gate(unreadable, ExclusionRules::default()).authorize(&PageId(LEAF.into())),
            Err(ScopeError::Denied)
        );
        let foreign = BTreeMap::from([
            entry(OTHER_ROOT, InventoryParent::Root),
            entry(LEAF, InventoryParent::Page(OTHER_ROOT.into())),
        ]);
        assert_eq!(
            gate(foreign, ExclusionRules::default()).authorize(&PageId(LEAF.into())),
            Err(ScopeError::Denied)
        );
    }
}
