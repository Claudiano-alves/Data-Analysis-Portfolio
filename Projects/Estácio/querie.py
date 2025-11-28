def get_query_tempos(dt_ini, dt_fim):
    query = f"""
        SELECT * 
        FROM OPENQUERY(EXPERT, '
        SELECT
            rah.nome AS ''NOME_AGENTE'',
            rah.dia AS ''DATA_LOGIN'',
            MIN(rah.login) AS ''PRIMEIRO_LOGIN'',
            rah.dia AS ''DATA_LOGOUT'',
            MAX(rah.logout) AS ''ULTIMO_LOGOUT'',
            TIMEDIFF(MAX(rah.logout), MIN(rah.login)) AS ''TP_LOGADO'',
            rah.tempo_falado_hora_grupo AS ''Produtivo'',

            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
            FROM tb_relatorio_pausa tp
            WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
            AND tp.DSC_AGENTE = rah.agente
            AND tp.DSC_PAUSA IN ( 
                ''10 minutos - primeira'',
                ''pausa 10 primeira'',
                ''1? pausa 10 min'',
                ''1ª pausa 10 min | expert'',
                ''1ª pausa/descanso(10min)'',
                ''descanso''
            ))  AS ''1a Pausa 10 minutos'',
                    
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (  
                ''1? pausa 10 min'',
                ''2? pausa 10 min'',
                ''pausa 10 segunda'',
                ''2ª pausa 10 min | expert'',
                ''10 minutos - segunda'',
                ''10 minutos - primeira'',
                ''pausa 10 primeira'',
                ''10 minutos - segunda'',
                ''2ª pausa 10 min | expert''
            )) AS ''2a Pausa 10 minutos'',
            
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (    
                ''almoÇo'',
                ''almoÇo'',
                ''almoço'',
                ''lanche 1hr''
            ))  AS ''Almoço'',

            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (    
                ''erro/sistema'',
                ''outros | expert''
            ))  AS ''Banheiro'',
            
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                    AND tp.DSC_AGENTE = rah.agente
                    AND tp.DSC_PAUSA IN (    
                ''treinamento'',
                ''feedback'',
                ''suporte'',
                ''reunião'',
                ''reuniao/trein'',
                ''pausasuper'',
                ''supervisao'',
                ''treinamento expert'',
                ''reuniao / treinamento von'',
                ''reuniao / treinamento''
            ))  AS ''FEEDBACK'',
            
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (    
                ''pausa 20 | expert'',
                ''pausa 20'',
                ''pausa lanche'',
                ''pausa lanche 20 min vonix''
            ))  AS ''Interlavo 20 minutos'',
            
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (    
                ''selecionando pausa'',
                ''whatsapp'',
                ''administrativa'',
                ''erro/sistema'',
                ''pausa - discagem'',
                ''reunião''
            )) AS ''Outras pausas''
        FROM relatorio_agentes_hora rah
            JOIN grupo g ON rah.grupoprincipal = g.id_grupo
        WHERE
            rah.dia BETWEEN ''{dt_ini}'' AND ''{dt_fim}''
            AND rah.grupoprincipal IN (4643, 4651, 4652, 4653, 4658, 4660, 4662, 4699)
        GROUP BY
            rah.dia,
            rah.agente
        ORDER BY
            rah.dia,
            rah.agente
        ')
        """
    return query