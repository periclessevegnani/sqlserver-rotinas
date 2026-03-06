-- O primeiro bloco atualiza estatisticas de todas as colunas de todas as tabelas com fullscan, que não tenham sido atualizadas nas ultimas 24h.
-- O segundo bloco, logo abaixo, cria indices de estatistica nas colunas que nao tenham ainda.

drop table #atualizar
go
select convert(varchar(80), '['+u.name+']'+'.'+'['+t.name+']') as tabela, '['+c.name+']' as coluna, i.rowcnt, d.last_updated
into #atualizar
from sys.stats c
 join sys.objects t
  on c.object_id = t.object_id
 join sysindexes i
  on i.id = t.object_id
 outer apply sys.dm_db_stats_properties(c.object_id, c.stats_id) d
 join sys.schemas u
  on t.schema_id = u.schema_id
where t.type = 'U'
  and substring(t.name,1,1) <> '@'
  and not i.name is null
  and d.last_updated < dateadd(hour,-24,getdate())
go
declare @tabela varchar(200)
declare @indice varchar(200)
declare @rowcnt int
declare @sql varchar(4000)
declare @sucesso int
declare @falha int
declare @ultima datetime
select @sucesso = 0
select @falha = 0
declare c_tabelas cursor fast_forward
for
  select distinct tabela, coluna, rowcnt
  from #atualizar
  where rowcnt > 0
  order by rowcnt
open c_tabelas
fetch next from c_tabelas into @tabela, @indice, @rowcnt
while @@fetch_status = 0
begin
  select @sql = 'update statistics '+@tabela+' ('+@indice+') with fullscan'
  select getdate() as horario, convert(varchar(50), db_name(db_id())) as database_name, convert(varchar(50), @tabela) as tabela, convert(varchar(15),@rowcnt) as linhas, @sql as comando
  begin try
    exec(@sql)
    select @sucesso = @sucesso + 1
  end try
  begin catch
    print error_message()
    select @falha = @falha + 1
  end catch
  fetch next from c_tabelas into @tabela, @indice, @rowcnt
end
close c_tabelas
deallocate c_tabelas
select convert(varchar(50), db_name(db_id())) as database_name, @sucesso as sucesso, @falha as falha
go

declare @tabela varchar(200)
declare @coluna varchar(200)
declare @rowcnt int
declare @sql varchar(4000)
declare @sucesso int
declare @falha int
select @sucesso = 0
select @falha = 0
declare c_tabelas cursor fast_forward
for
  select convert(varchar(80), '['+u.name+']'+'.'+'['+t.name+']') as tabela, '['+c.name+']' as coluna, max(i.rowcnt) as qtde
  from sysobjects t (nolock)
   inner join sys.columns c (nolock)
    on t.id = c.object_id
   inner join sys.schemas u (nolock)
    on t.uid = u.schema_id
   inner join sysindexes i (nolock)
    on i.id = t.id
  where t.type = 'U'
    and not exists (select *
                    from sys.stats st (nolock)
                    where st.object_id = t.id
                      and st.name = c.name)
    and c.is_computed = 0
  group by convert(varchar(80), '['+u.name+']'+'.'+'['+t.name+']'), '['+c.name+']'
  having max(i.rowcnt) > 0
  order by qtde, tabela, coluna
open c_tabelas
fetch next from c_tabelas into @tabela, @coluna, @rowcnt
while @@fetch_status = 0
begin
  select @sql = 'create statistics '+@coluna+' on '+@tabela+' ('+@coluna+') with fullscan'
  select getdate() as horario, convert(varchar(50), db_name(db_id())) as database_name, convert(varchar(50), @tabela) as tabela, convert(varchar(50), @coluna) as coluna, convert(varchar(15),@rowcnt) as linhas, @sql as comando
  begin try
    exec(@sql)
    select @sucesso = @sucesso + 1
  end try
  begin catch
    print error_message()
    select @falha = @falha + 1
  end catch
  fetch next from c_tabelas into @tabela, @coluna, @rowcnt
end
close c_tabelas
deallocate c_tabelas
select convert(varchar(50), db_name(db_id())) as database_name, @sucesso as sucesso, @falha as falha
go
