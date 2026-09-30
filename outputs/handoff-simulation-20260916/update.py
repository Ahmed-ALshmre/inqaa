from pathlib import Path
p=Path('outputs/handoff-simulation-20260916/simulate.py');s=p.read_text(encoding='utf-8')
s=s.replace("     try:\n      with ExitStack()", """     linked=m.load_customer_products(db,ev['sender_id'])
     item[mode]['linked_products']=len(linked)
     item[mode]['direct_catalog_answer_available']=bool(m.known_product_question_reply(ev['text'],linked[0])) if len(linked)==1 else False
     item[mode]['pending_image_messages']=db.execute(\"SELECT count(*) FROM messages WHERE sender_id=? AND direction='incoming' AND (message_type='image' OR image_url IS NOT NULL AND image_url!='') AND id>COALESCE((SELECT max(id) FROM messages WHERE sender_id=? AND direction='outgoing'),0)\",(ev['sender_id'],ev['sender_id'])).fetchone()[0]
     try:
      with ExitStack()""")
s=s.replace("  print(json.dumps(totals['categories'],ensure_ascii=False))", "  print(json.dumps(totals['categories'],ensure_ascii=False))\n  source.close()")
p.write_text(s,encoding='utf-8')
