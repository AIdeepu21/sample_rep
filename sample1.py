async def _fetch_sql_aggregates(
    self, session: AsyncSession, pfr_ids: List[int]
) -> Dict[int, Dict[str, Any]]:
    if not pfr_ids:
        return {}

    R = pfrconstants.Permission
    D = pfrconstants.DocumentTypes

    role_q = (
        select(
            literal("role").label("_type"),
            PFRFormUsers.pfrBasicDetailsId.label("pfr_id"),
            Role.roleName,
            PFRFormUsers.userName,
            PFRFormUsers.userPhone,
            PFRFormUsers.userCellNumber,
            PFRFormUsers.department,
            UserProfile.department.label("profileDepartment"),
            literal(None).label("totalHookup"),
            literal(None).label("totalFacilities"),
            literal(None).label("docTypeName"),
            literal(None).label("hasRequired"),
        )
        .join(Role, Role.id == PFRFormUsers.userRole, isouter=True)
        .join(UserProfile, UserProfile.userName == PFRFormUsers.userId, isouter=True)
        .where(
            PFRFormUsers.pfrBasicDetailsId.in_(pfr_ids),
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
            literal("budget").label("_type"),
            PFRBudgets.pfrBasicDetailsId.label("pfr_id"),
            literal(None).label("roleName"),
            literal(None).label("userName"),
            literal(None).label("userPhone"),
            literal(None).label("userCellNumber"),
            literal(None).label("department"),
            literal(None).label("profileDepartment"),
            func.sum(
                func.coalesce(PFRBudgets.hookupCapital, 0)
                + func.coalesce(PFRBudgets.hookupExpense, 0)
            ).label("totalHookup"),
            func.sum(
                func.coalesce(PFRBudgets.facilitiesCapital, 0)
                + func.coalesce(PFRBudgets.facilitiesExpense, 0)
            ).label("totalFacilities"),
            literal(None).label("docTypeName"),
            literal(None).label("hasRequired"),
        )
        .where(PFRBudgets.pfrBasicDetailsId.in_(pfr_ids))
        .group_by(PFRBudgets.pfrBasicDetailsId)
    )

    doc_q = (
        select(
            literal("doc").label("_type"),
            PFRDocuments.pfrBasicDetailsId.label("pfr_id"),
            literal(None).label("roleName"),
            literal(None).label("userName"),
            literal(None).label("userPhone"),
            literal(None).label("userCellNumber"),
            literal(None).label("department"),
            literal(None).label("profileDepartment"),
            literal(None).label("totalHookup"),
            literal(None).label("totalFacilities"),
            Keywords.value.label("docTypeName"),
            func.max(case((PFRDocuments.isRequired == True, 1), else_=0)).label("hasRequired"),
        )
        .join(Keywords, Keywords.id == PFRDocuments.documentType, isouter=True)
        .where(
            PFRDocuments.pfrBasicDetailsId.in_(pfr_ids),
            Keywords.value.in_([
                D.BU_REQUIREMENTS, D.SSPS,
                D.NON_APPLIED_CATALOG, D.DRAWINGS, D.OTHERS,
            ])
        )
        .group_by(PFRDocuments.pfrBasicDetailsId, Keywords.value)
    )

    # ONE round trip — same DB, no reason for 3 connections
    union_q = union_all(role_q, budget_q, doc_q)

    t0 = time.perf_counter()
    result = await session.execute(union_q)  # reuse the passed-in session
    rows = result.all()
    print(f"[PERF] Single query: {time.perf_counter() - t0:.3f}s")

    # Split and assemble exactly as before
    role_rows = [r for r in rows if r._type == "role"]
    bud_rows  = [r for r in rows if r._type == "budget"]
    doc_rows  = [r for r in rows if r._type == "doc"]

    agg: Dict[int, Dict[str, Any]] = {}
    self._apply_role_rows(agg, role_rows, R)
    self._apply_budget_rows(agg, bud_rows)
    self._apply_doc_rows(agg, doc_rows, pfr_ids, D)
    return agg