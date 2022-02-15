from github import Commit, Repository


class CommitModel():

    @staticmethod
    def to_dict(commit: Commit, repository: Repository) -> dict:
        commit = {
            'commiter': {'login': commit.committer.login},
            'author': {'login': commit.author.login},
            'url': commit.url,
            'sha': commit.sha,
            'committer_date': commit.commit.committer.date.isoformat(),
            'author_date': commit.commit.author.date.isoformat(),
            'repository': {
                'full_name': repository.full_name,
                'language': repository.language,
                'stargazers_count': repository.stargazers_count,
                'forks_count': repository.forks_count}
        }
        return commit
