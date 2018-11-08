## Insert overWrite需要create权限

http://jira.netease.com/browse/NESPARK-161

```
INSERT OVERWRITE TABLE haitao_open.adi_kl_pm_usr_goods_act_preference_df PARTITION (day='2018-10-30') select 
```

```
org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException: Permission denied: user [bdms_hzliutao1] does not have [CREATE] privilege on [haitao_open/adi_kl_pm_usr_goods_act_preference_df/account_id,goods_id,activity_type,rank,push_receive_status,sms_receive_status,day]
```

  经过定位,权限校验是在 spark sql 的optimize阶段。

```
override def apply(plan: LogicalPlan): LogicalPlan = {
  val hiveOperationType = toHiveOperationType(plan)
  val hiveAuthzContext = getHiveAuthzContext(plan)
  SparkSession.getActiveSession match {
    case Some(session) =>
      session.sharedState.externalCatalog match {
        case catalog: HiveExternalCatalog =>
          catalog.client match {
            case authz: Authorizable =>
              val (in, out) = HivePrivObjsFromPlan.build(plan, authz.currentDatabase())
              authz.checkPrivileges(hiveOperationType, in, out, hiveAuthzContext)
            case _ =>
              val (in, out) = HivePrivObjsFromPlan.build(plan, defaultAuthz.currentDatabase())
              defaultAuthz.checkPrivileges(hiveOperationType, in, out, hiveAuthzContext)
          }
        case _ =>
      }
    case None =>
  }
```

这是一个规则，首先是确定 in,out操作，然后进行权限校验。

关注这个(in, out)

我怀疑，这里在判断insertIntoTable的时候，没有care overwrite。

```scala
case InsertIntoTable(table, _, child, _, _) =>
  // table is a logical plan not catalogTable, so miss overwrite and partition info.
  // TODO: deal with overwrite
  buildUnaryHivePrivObjs(table, currentDb, outputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)
  buildUnaryHivePrivObjs(child, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)
```

```scala
case LogicalRelation(_, _, Some(table)) =>
  handleProjectionForRelation(table)
```

接下来调用的这个处理relation。

```scala
def handleProjectionForRelation(table: CatalogTable): Unit = {
  if (projectionList == null) {
    addTableOrViewLevelObjs(
      table.identifier,
      hivePrivilegeObjects,
      currentDb,
      table.partitionColumnNames.asJava,
      table.schema.fieldNames.toList.asJava)
  } else if (projectionList.isEmpty) {
    addTableOrViewLevelObjs(table.identifier, hivePrivilegeObjects, currentDb)
  } else {
    addTableOrViewLevelObjs(
      table.identifier,
      hivePrivilegeObjects,
      currentDb,
      table.partitionColumnNames.filter(projectionList.map(_.name).contains(_)).asJava,
      projectionList.map(_.name).asJava)
  }
}
```

然后加入tableOrViewLevelObjs

```scala
private def addTableOrViewLevelObjs(
    tableIdentifier: TableIdentifier,
    hivePrivilegeObjects: JList[HivePrivilegeObject],
    currentDb: String,
    partKeys: JList[String] = null,
    columns: JList[String] = null,
    mode: SaveMode = SaveMode.ErrorIfExists,
    cmdParams: JList[String] = null): Unit = {
  val dbName = tableIdentifier.database.getOrElse(currentDb)
  val tbName = tableIdentifier.table
  val hivePrivObjectActionType = getHivePrivObjActionType(mode)
  hivePrivilegeObjects.add(
    HivePrivilegeObjectHelper(
      HivePrivilegeObjectType.TABLE_OR_VIEW,
      dbName,
      tbName,
      partKeys,
      columns,
      hivePrivObjectActionType,
      cmdParams))
}
```

这里判断了,   val hivePrivObjectActionType = getHivePrivObjActionType(mode).

因为mode之前一直是忽略的。

```scala
private def getHivePrivObjActionType(mode: SaveMode): HivePrivObjectActionType = {
  mode match {
    case SaveMode.Append => HivePrivObjectActionType.INSERT
    case SaveMode.Overwrite => HivePrivObjectActionType.INSERT_OVERWRITE
    case _ => HivePrivObjectActionType.OTHER
  }
}
```

因此，这里本应该是获得insert_overWrite，结果，被判断成了Other。







## Others

```scala
  private def overwriteToSaveMode(overwrite: Boolean): SaveMode = {
    if (overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.ErrorIfExists
    }
  }
```

这个方法是否合适，如果是append呢？



## Savemod

```scala
case class CreateTable(
    tableDesc: CatalogTable,
    mode: SaveMode,
    query: Option[LogicalPlan]) extends Command {
  assert(tableDesc.provider.isDefined, "The table to be created must have a provider.")

  if (query.isEmpty) {
    assert(
      mode == SaveMode.ErrorIfExists || mode == SaveMode.Ignore,
      "create table without data insertion can only use ErrorIfExists or Ignore as SaveMode.")
  }

  override def innerChildren: Seq[QueryPlan[_]] = query.toSeq
}
```

这里只能是ErrorIfExists或者Ignore

