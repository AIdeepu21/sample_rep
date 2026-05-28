async def _fetch_sql_aggregates(
    self, session: AsyncSession, pfr_ids: List[int]
) -> Dict[int, Dict[str, Any]]:
    if not pfr_ids:
        return {}

    R = pfrconstants.Permission
    D = pfrconstants.DocumentTypes

    # ── Step 1: Create temp table and bulk insert IDs once ──
    await session.execute(text("""
        CREATE TABLE #pfr_filter (pfr_id INT NOT NULL PRIMARY KEY)
    """))

    # Chunk insert to stay safe (500 per insert)
    chunk_size = 500
    for i in range(0, len(pfr_ids), chunk_size):
        chunk = pfr_ids[i:i + chunk_size]
        placeholders = ",".join(f"({v})" for v in chunk)
        await session.execute(text(f"""
            INSERT INTO #pfr_filter (pfr_id) VALUES {placeholders}
        """))

    # ── Step 2: All 3 queries JOIN against temp table ──
    union_q = text("""
        SELECT
            'role'  AS _type,
            u.pfrBasicDetailsId AS pfr_id,
            r.roleName, u.userName, u.userPhone,
            u.userCellNumber, u.department,
            p.department AS profileDepartment,
            NULL AS totalHookup, NULL AS totalFacilities,
            NULL AS docTypeName, NULL AS hasRequired
        FROM PFRFormUsers u
        JOIN #pfr_filter f ON f.pfr_id = u.pfrBasicDetailsId
        LEFT JOIN Role r ON r.id = u.userRole
        LEFT JOIN UserProfile p ON p.userName = u.userId
        WHERE u.active = 1
          AND r.roleName IN (
            :r1,:r2,:r3,:r4,:r5,:r6,:r7
          )

        UNION ALL

        SELECT
            'budget' AS _type,
            b.pfrBasicDetailsId AS pfr_id,
            NULL, NULL, NULL, NULL, NULL, NULL,
            SUM(COALESCE(b.hookupCapital,0) + COALESCE(b.hookupExpense,0))   AS totalHookup,
            SUM(COALESCE(b.facilitiesCapital,0) + COALESCE(b.facilitiesExpense,0)) AS totalFacilities,
            NULL, NULL
        FROM PFRBudgets b
        JOIN #pfr_filter f ON f.pfr_id = b.pfrBasicDetailsId
        GROUP BY b.pfrBasicDetailsId

        UNION ALL

        SELECT
            'doc' AS _type,
            d.pfrBasicDetailsId AS pfr_id,
            NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL,
            k.value AS docTypeName,
            MAX(CASE WHEN d.isRequired = 1 THEN 1 ELSE 0 END) AS hasRequired
        FROM PFRDocuments d
        JOIN #pfr_filter f ON f.pfr_id = d.pfrBasicDetailsId
        LEFT JOIN Keywords k ON k.id = d.documentType
        WHERE k.value IN (:d1,:d2,:d3,:d4,:d5)
        GROUP BY d.pfrBasicDetailsId, k.value
    """)

    result = await session.execute(union_q, {
        # role names
        "r1": R.USERROLE_ORIGINATOR,
        "r2": R.USERROLE_REQUESTOR,
        "r3": R.USERROLE_FPM,
        "r4": R.USERROLE_PLANM,
        "r5": R.USERROLE_PFR_PLANM,
        "r6": R.USERROLE_FEASIBILILTY_PLANNER,
        "r7": R.USERROLE_CC,
        # doc types
        "d1": D.BU_REQUIREMENTS,
        "d2": D.SSPS,
        "d3": D.NON_APPLIED_CATALOG,
        "d4": D.DRAWINGS,
        "d5": D.OTHERS,
    })

    rows = result.all()

    # ── Step 3: Split and assemble exactly as before ──
    role_rows = [r for r in rows if r._type == "role"]
    bud_rows  = [r for r in rows if r._type == "budget"]
    doc_rows  = [r for r in rows if r._type == "doc"]

    agg: Dict[int, Dict[str, Any]] = {}
    self._apply_role_rows(agg, role_rows, R)
    self._apply_budget_rows(agg, bud_rows)
    self._apply_doc_rows(agg, doc_rows, pfr_ids, D)
    return agg