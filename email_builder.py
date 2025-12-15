def build_email(jobs):
    html = "<h2>Vagas mais aderentes ao seu perfil</h2>"

    top3 = jobs[:3]
    for i, job in enumerate(top3, start=1):
        html += f"""
        <p>
        <b>{i}. {job['title']}</b><br>
        {job.get('source')} — {job.get('country')}<br>
        <a href="{job.get('url')}">Link da vaga</a>
        </p>
        """

    html += "<hr><h3>Demais oportunidades</h3>"
    for job in jobs[3:]:
        html += f"""
        <p>
        <b>{job['title']}</b> — {job.get('source')} ({job.get('country')})<br>
        <a href="{job.get('url')}">Link</a>
        </p>
        """

    return html
