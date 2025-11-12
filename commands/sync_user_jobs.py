from utils.scheduler import scheduler


async def sync_user_product_jobs():
    scheduler.start()

    # Получаем все задачи
    for job in scheduler.get_jobs():
        if ":ozon:" in job.id or ":wb:" in job.id:
            args = list(job.args)
            if (
                args
                and isinstance(args[0], str)
                and args[0].startswith("push_check_")
                and args[0].endswith("_price")
            ):
                # Меняем только первый аргумент
                args[0] = "push_check_price"

                # Пересоздаём задачу с обновлёнными аргументами
                scheduler.add_job(
                    func=job.func,
                    trigger=job.trigger,
                    id=job.id,
                    coalesce=job.coalesce,
                    args=args,
                    kwargs=job.kwargs,
                    jobstore="sqlalchemy",
                    replace_existing=True,
                    next_run_time=job.next_run_time,
                    misfire_grace_time=job.misfire_grace_time,
                    max_instances=job.max_instances,
                )

    scheduler.shutdown()
