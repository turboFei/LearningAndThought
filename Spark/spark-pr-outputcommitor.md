# PR Review- OutputCommitCoordinator may allow duplicate commits

## 问题

在spark中存在stage重试机制，当一个stage被判定为失败需要重试时，当前stage中正在运行的task不会被cancel，而是继续运行，这时如果这些已经failstage中的task向hdfs中写数据，会造成数据输出双份。

## 背景

- spark stage中的容错机制

在spark中，任务是一个stage中若干个(等待被计算的partition数量）相互独立的task。 在一个stage中，如果是一个task在执行过程中出现了fetch failed错误，task fetch的数据 是map端写入的数据，一个task读取的数据需要对应到上一个stage map的写数据，因此，出现了fetch failed时，会关联到前面stage的重新计算，因此就不只是重新计算本个stage中的任务这么简单，会调用resubmitFailedStage操作，后面会讲解。

如果只是出现普通错误，比如在执行过程中出现异常，只需要将这个task，重新加入该stage的pending task集合中，然后对该task的出现错误次数进行判定，如果该task已经超过参数`spark.task.maxFailures`,那么对该stage进行abort操作。

如果是出现了executor lost，操作则是进行resubmit操作，也就是重新提交该task。 还有其他错误，dagscheduler会进行相应的处理。

### resubmitFailedStage

前面处理错误的几种方式中，最特殊的就是resubmitFailedStage。其他几种方式都只是重新计算task，只需要从该stage开始的地方读数据，然后重新计算即可。 而fetch failed，是读数据就出错了，因此需要重新计算该task依赖的partition链。 那么在resubmitFailedStage时，是否会对这个stage中已经成功执行的task进行重新计算？ 这里已经成功执行的task代表是已经成功写入数据(shuffleMapTask的 write或者resultTask的result输出)的task，`而不包括那些正在运行还没出错，而且将来也可能会运行成功的task`。

#### 重新提交failedStage会运行哪些任务，是否全量运行？

首先了解下fetch failed。 fetch failed 是在`FetchFailedException`这个类中产生的一个reason。 而在BlockShuffleReader中会报这个异常，报异常时，信息如下。 throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e) 可以看出此处会报具体对应的mapId，也就是在fetch数据时，只是这个mapId对应task写数据出错，那就不需要计算上个stage所有的任务，只需要计算mapId对应task的结果。

下面简单用语言描述下dagScheduler种对fetch failed的处理过程. `case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage)`

首先是failedStage表示当前失败的stage，然后mapStage表示前一个stage。 某些if条件此处省去，然后是将failedStage和mapStage加入faildStages集合里面。

```
if (mapId != -1) {

          mapStage.removeOutputLoc(mapId, bmAddress)

          mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)

        }
```

然后是将mapId对应的分区的outputLoc标志位重置，因为outputLoc是一个shuffleMapStage判断还没进行计算的partition的标志集合，如果其对应的outputLoc是空的，就是未计算的partition，才会生成对应的task进行计算。而unregisterMapOutput操作则是清空mapId对应task的mapOutPut数据。 因此mapStage只需要计算涉及到的任务。而failedStage，则是重新计算除了已经计算成功的，包括那些正在执行还没失败，也可能不会失败的任务。 因此reSubmitFailedStage时进行的计算不是全量的，是轻量的针对失败分区的计算。

#### 什么时候发生问题

因此这些正在执行的任务，加上重新提交stage的任务，如果是向hdfs写数据，如果执行成功，可能会写双份数据。

## 修改

修改就是给taskContext加一个`stageAttemptId`字段，通过判断当前任务是否是最新attempt的任务，来给outputCommit的权限。这样就避免输出双份数据。