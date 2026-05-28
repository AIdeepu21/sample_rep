async def _fetch_sql_aggregates(
    self, session: AsyncSession, pfr_ids: List[int]
) -> Dict[int, Dict[str, Any]]:
    if not pfr_ids:
        return {}

    R = pfrconstants.Permission
    D = pfrconstants.DocumentTypes
    CHUNK_SIZE = 700

    _session_factory = get_async_sessionmaker()

    # ── One isolated session per query ──
    async def _run_isolated(stmt):
        async with _session_factory() as _s:        # own connection
            async with _s.begin():                  # own transaction
                result = await _s.execute(stmt)
                return result.all()                 # collected before session closes

    chunks = [
        pfr_ids[i:i + CHUNK_SIZE]
        for i in range(0, len(pfr_ids), CHUNK_SIZE)
    ]

    # ── Build all statements first, then fire all at once ──
    tasks = []
    for chunk in chunks:
        role_q = (
            select(
                PFRFormUsers.pfrBasicDetailsId,
                Role.roleName,
                PFRFormUsers.userName,
                PFRFormUsers.userPhone,
                PFRFormUsers.userCellNumber,
                PFRFormUsers.department,
                UserProfile.department.label("profileDepartment"),
            )
            .join(Role, Role.id == PFRFormUsers.userRole, isouter=True)
            .join(UserProfile, UserProfile.userName == PFRFormUsers.userId, isouter=True)
            .where(
                PFRFormUsers.pfrBasicDetailsId.in_(chunk),
                PFRFormUsers.active == True,
                Role.roleName.in_([
                    R.USERROLE_ORIGINATOR, R.USERROLE_REQUESTOR,
                    R.USERROLE_FPM, R.USERROLE_PLANM,
                    R.USERROLE_PFR_PLANM, R.USERROLE_FEASIBILILTY_PLANNER,
                    R.USERROLE_CC,
                ])
            )
        )

        budget_q = (
            select(
                PFRBudgets.pfrBasicDetailsId,
                func.sum(
                    func.coalesce(PFRBudgets.hookupCapital, 0)
                    + func.coalesce(PFRBudgets.hookupExpense, 0)
                ).label("totalHookup"),
                func.sum(
                    func.coalesce(PFRBudgets.facilitiesCapital, 0)
                    + func.coalesce(PFRBudgets.facilitiesExpense, 0)
                ).label("totalFacilities"),
            )
            .where(PFRBudgets.pfrBasicDetailsId.in_(chunk))
            .group_by(PFRBudgets.pfrBasicDetailsId)
        )

        doc_q = (
            select(
                PFRDocuments.pfrBasicDetailsId,
                Keywords.value.label("docTypeName"),
                func.max(
                    case((PFRDocuments.isRequired == True, 1), else_=0)
                ).label("hasRequired"),
            )
            .join(Keywords, Keywords.id == PFRDocuments.documentType, isouter=True)
            .where(
                PFRDocuments.pfrBasicDetailsId.in_(chunk),
                Keywords.value.in_([
                    D.BU_REQUIREMENTS, D.SSPS,
                    D.NON_APPLIED_CATALOG, D.DRAWINGS, D.OTHERS,
                ])
            )
            .group_by(PFRDocuments.pfrBasicDetailsId, Keywords.value)
        )

        # 3 isolated tasks per chunk
        tasks.append(_run_isolated(role_q))
        tasks.append(_run_isolated(budget_q))
        tasks.append(_run_isolated(doc_q))

    # ── All fire in parallel, each with its own connection ──
    import time
    t0 = time.perf_counter()
    all_results = await asyncio.gather(*tasks)
    print(f"[PERF] All isolated queries gathered: {time.perf_counter() - t0:.3f}s")

    # ── Unpack: results come back as [r0, b0, d0, r1, b1, d1, ...] ──
    role_rows, bud_rows, doc_rows = [], [], []
    for i, rows in enumerate(all_results):
        bucket = i % 3  # 0=role, 1=budget, 2=doc
        if bucket == 0:
            role_rows.extend(rows)
        elif bucket == 1:
            bud_rows.extend(rows)
        else:
            doc_rows.extend(rows)

    # ── Assemble — identical to before ──
    agg: Dict[int, Dict[str, Any]] = {}
    self._apply_role_rows(agg, role_rows, R)
    self._apply_budget_rows(agg, bud_rows)
    self._apply_doc_rows(agg, doc_rows, pfr_ids, D)
    return agg